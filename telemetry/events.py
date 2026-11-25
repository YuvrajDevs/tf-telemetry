"""Event contract and pure domain logic (no Spark, no Kafka): unit-testable in milliseconds."""
import json
import math
import uuid
from datetime import datetime, timezone

from .cmapss import SENSORS

SCHEMA_VERSION = 1
NAMESPACE = uuid.UUID("6f1d6a2e-2b6a-4f7e-9a51-3c1b2b9d7e10")

# Sensors that drift with wear, and their direction (+1 rises, -1 falls as the engine degrades).
# Baselines = mean / std of the first 30 (healthy) cycles of every FD001 engine.
HEALTH_SENSORS = {
    "s2": (+1, 642.3832, 0.3795), "s3": (+1, 1586.8985, 4.6482), "s4": (+1, 1402.831, 5.9308),
    "s7": (-1, 553.9477, 0.6237), "s11": (+1, 47.3561, 0.1769), "s12": (-1, 521.9049, 0.5068),
    "s15": (+1, 8.4184, 0.0264), "s17": (+1, 392.27, 1.1424), "s20": (-1, 38.9323, 0.1296),
    "s21": (-1, 23.3592, 0.0787),
}

# C-MAPSS FD003 has a SECOND fault mode (fan degradation). Its signature differs from the HPC fault above:
# s7, s12, s20, s21 rise instead of falling and s15 falls instead of rising (build-log F14, Milestone T1).
FAN_SIGNS = {"s2": +1, "s3": +1, "s4": +1, "s7": +1, "s11": +1, "s12": +1,
             "s15": -1, "s17": +1, "s20": +1, "s21": +1}
# The fan pattern moves s7/s12 by 20+ standard deviations, so it is down-weighted to give the same lead times and
# false-alarm rate as the HPC pattern. Tuned on odd FD003 engines, checked on even ones (scripts/calibrate.py).
FAN_WEIGHT = 0.4

# Physically plausible ranges. Anything outside is a sensor/transport fault -> dead-letter.
SENSOR_RANGES = {s: (0.0, 20000.0) for s in SENSORS}
SENSOR_RANGES.update({"s11": (40.0, 60.0), "s4": (1200.0, 1600.0), "s2": (600.0, 700.0)})

# Alert policy, calibrated on FD001 run-to-failure data (see docs/decisions.md, ADR-004):
EWMA_ALPHA = 0.2
WARNING_AT = 1.5     # median lead ~44 cycles before failure
CRITICAL_AT = 2.5    # median lead ~19 cycles before failure
LEVELS = {0: "OK", 1: "WARNING", 2: "CRITICAL"}


def engine_id(subset: str, unit: int, loop: int = 0) -> str:
    return f"{subset}-{unit:03d}" + (f"-L{loop}" if loop else "")


def event_id(engine: str, cycle: int) -> str:
    """Deterministic: the same engine+cycle always gets the same id, so duplicates are detectable."""
    return str(uuid.uuid5(NAMESPACE, f"{engine}:{cycle}"))


def make_event(subset: str, row: dict, loop: int = 0, now: datetime | None = None) -> dict:
    eng = engine_id(subset, row["unit"], loop)
    now = now or datetime.now(timezone.utc)
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id(eng, row["cycle"]),
        "fleet": subset,
        "engine_id": eng,
        "cycle": int(row["cycle"]),
        "event_time": now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "op_settings": [row["op_setting_1"], row["op_setting_2"], row["op_setting_3"]],
        "sensors": {s: row[s] for s in SENSORS},
    }


def to_bytes(event: dict) -> bytes:
    return json.dumps(event, separators=(",", ":")).encode()


def validate(event: dict | None) -> str | None:
    """Return None if valid, else a dead-letter reason code. Mirrors the Spark silver rules."""
    if not isinstance(event, dict):
        return "MALFORMED_JSON"
    for f in ("event_id", "engine_id", "cycle", "event_time", "sensors"):
        if event.get(f) in (None, ""):
            return "MISSING_FIELD"
    sensors = event["sensors"]
    for s in SENSORS:
        v = sensors.get(s)
        if v is None or (isinstance(v, float) and math.isnan(v)):
            return "MISSING_FIELD"
        lo, hi = SENSOR_RANGES[s]
        if not lo <= v <= hi:
            return "OUT_OF_RANGE"
    return None


def _index(sensors: dict, signs: dict) -> float:
    total = 0.0
    for s, (_, mu, sd) in HEALTH_SENSORS.items():
        total += signs[s] * (sensors[s] - mu) / sd
    return total / len(HEALTH_SENSORS)


HPC_SIGNS = {s: sign for s, (sign, _, _) in HEALTH_SENSORS.items()}


def hpc_index(sensors: dict) -> float:
    """Signed z-score average for the HPC (compressor) fault pattern."""
    return _index(sensors, HPC_SIGNS)


def fan_index(sensors: dict) -> float:
    """Signed z-score average for the fan fault pattern."""
    return _index(sensors, FAN_SIGNS)


def health_index(sensors: dict) -> float:
    """Fault-mode aware health index (v2): the worse of the HPC and fan patterns.

    ~0 when healthy, rises with wear of either kind. v1 used only the HPC pattern, so fan-fault
    engines cancelled themselves out and 44% of FD003 never alerted (build-log F14).
    """
    return max(hpc_index(sensors), FAN_WEIGHT * fan_index(sensors))


def fault_mode(sensors: dict) -> str:
    """Which pattern dominates: useful for routing the alert to the right maintenance team."""
    return "FAN" if FAN_WEIGHT * fan_index(sensors) > hpc_index(sensors) else "HPC"


def level_for(ewma: float) -> int:
    return 2 if ewma >= CRITICAL_AT else 1 if ewma >= WARNING_AT else 0


def advance_state(prev: dict | None, events: list[dict]) -> tuple[dict | None, list[dict]]:
    """Fold a batch of one engine's events into its state. Returns (new_state, alerts_raised).

    - Events are applied in cycle order.
    - Events at or before the last applied cycle are ignored: this makes re-processing a
      micro-batch (after a crash) idempotent, and drops stale out-of-order cycles.
    - An alert is raised only when the level escalates (OK->WARNING, ->CRITICAL).
    """
    state = dict(prev) if prev else None
    alerts = []
    for e in sorted(events, key=lambda x: x["cycle"]):
        if state and e["cycle"] <= state["last_cycle"]:
            continue
        hi = e["health_index"]
        ewma = hi if not state else EWMA_ALPHA * hi + (1 - EWMA_ALPHA) * state["ewma_health"]
        prev_level = state["alert_level"] if state else 0
        lvl = max(prev_level, level_for(ewma))  # levels only escalate until maintenance resets the engine
        state = {"engine_id": e["engine_id"], "fleet": e["fleet"], "last_cycle": e["cycle"],
                 "last_event_time": e["event_time"], "health_index": hi, "ewma_health": ewma,
                 "alert_level": lvl, "s11": e["s11"], "s4": e["s4"], "fault_mode": e.get("fault_mode")}
        if lvl > prev_level:
            alerts.append({"alert_id": f"{e['engine_id']}:{LEVELS[lvl]}", "engine_id": e["engine_id"], "fleet": e["fleet"],
                           "level": LEVELS[lvl], "cycle": e["cycle"], "ewma_health": ewma, "event_time": e["event_time"],
                           "fault_mode": e.get("fault_mode")})
    return state, alerts
