"""Fast unit tests of the pure domain logic (no Spark, no Kafka)."""
import json
from datetime import datetime, timezone

import pytest

from telemetry import cmapss
from telemetry.events import (CRITICAL_AT, WARNING_AT, advance_state, event_id, health_index, make_event, to_bytes, validate)

ROW = cmapss.synthetic("FD001", engines=1)[0]
NOW = datetime(2026, 11, 25, 8, 0, tzinfo=timezone.utc)


def ev(cycle, hi, engine="FD001-001"):
    return {"engine_id": engine, "fleet": "FD001", "cycle": cycle, "event_time": f"t{cycle}", "health_index": hi, "s11": 47.5, "s4": 1405.0}


def test_event_id_is_deterministic():
    assert event_id("FD001-001", 5) == event_id("FD001-001", 5)
    assert event_id("FD001-001", 5) != event_id("FD001-001", 6)


def test_make_event_roundtrip_is_valid():
    e = make_event("FD001", ROW, now=NOW)
    assert e["engine_id"] == "FD001-001" and e["event_time"] == "2026-11-25T08:00:00.000Z"
    assert validate(json.loads(to_bytes(e))) is None


@pytest.mark.parametrize("mutate,reason", [
    (lambda e: None, "MALFORMED_JSON"),
    (lambda e: {k: v for k, v in e.items() if k != "engine_id"}, "MISSING_FIELD"),
    (lambda e: {**e, "sensors": {**e["sensors"], "s11": -1.0}}, "OUT_OF_RANGE"),
])
def test_validate_reasons(mutate, reason):
    assert validate(mutate(make_event("FD001", ROW, now=NOW))) == reason


def test_health_index_near_zero_when_healthy():
    assert abs(health_index(ROW)) < 1.5


def test_state_escalates_and_raises_each_alert_once():
    events = [ev(1, 0.0), ev(2, 0.0), ev(3, 10.0), ev(4, 10.0), ev(5, 10.0), ev(6, 10.0)]
    state, alerts = advance_state(None, events)
    assert [a["level"] for a in alerts] == ["WARNING", "CRITICAL"]
    assert state["alert_level"] == 2 and state["last_cycle"] == 6
    assert state["ewma_health"] >= CRITICAL_AT > WARNING_AT


def test_replaying_a_batch_is_idempotent():
    s1, a1 = advance_state(None, [ev(1, 0.0), ev(2, 9.0), ev(3, 9.0)])
    s2, a2 = advance_state(s1, [ev(1, 0.0), ev(2, 9.0), ev(3, 9.0)])   # same batch again after a crash
    assert s2 == s1 and a2 == []


def test_out_of_order_events_are_applied_in_cycle_order_and_stale_ones_ignored():
    s, _ = advance_state(None, [ev(3, 1.0), ev(1, 0.0), ev(2, 0.5)])
    assert s["last_cycle"] == 3
    s2, _ = advance_state(s, [ev(2, 99.0)])                            # stale cycle arrives late
    assert s2 == s


def test_synthetic_engines_degrade():
    rows = [r for r in cmapss.synthetic("FD001", engines=1)]
    assert health_index(rows[-1]) > health_index(rows[0]) + 2


def _shift(pattern_signs, z):
    """A healthy row with every health sensor moved z standard deviations in the given pattern's direction."""
    from telemetry.events import HEALTH_SENSORS
    row = dict(ROW)
    for s, (_, mu, sd) in HEALTH_SENSORS.items():
        row[s] = mu + pattern_signs[s] * z * sd
    return row


def test_fan_fault_is_detected_and_labelled():
    # v1 bug (build-log F14): the fan pattern cancelled itself out in the HPC-only index.
    from telemetry.events import FAN_SIGNS, fault_mode, hpc_index
    fan_row = _shift(FAN_SIGNS, 8.0)
    assert hpc_index(fan_row) < WARNING_AT          # the old index would miss it
    assert health_index(fan_row) >= CRITICAL_AT     # the new one does not
    assert fault_mode(fan_row) == "FAN"


def test_hpc_fault_is_labelled_hpc():
    from telemetry.events import HPC_SIGNS, fault_mode
    hpc_row = _shift(HPC_SIGNS, 4.0)
    assert health_index(hpc_row) >= CRITICAL_AT and fault_mode(hpc_row) == "HPC"


def test_alert_carries_fault_mode():
    events = [{**ev(c, 10.0), "fault_mode": "FAN"} for c in range(1, 6)]
    _, alerts = advance_state(None, events)
    assert {a["fault_mode"] for a in alerts} == {"FAN"}
