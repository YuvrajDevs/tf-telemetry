"""Offline alert calibration on NASA C-MAPSS run-to-failure data (Milestone T1).

Replays every engine cycle by cycle through the SAME EWMA + threshold logic the stream uses, and reports for each
health-index design: how many engines get a WARNING / CRITICAL before failure, how far ahead (lead time in cycles),
and how many warnings are "too early" (> EARLY cycles before failure, i.e. while the engine is still healthy).

    python scripts/calibrate.py            # compare designs on FD001 + FD003
"""
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from telemetry import events  # noqa: E402
from telemetry.cmapss import load  # noqa: E402

EARLY = 125  # C-MAPSS engines are usually treated as healthy until ~125 cycles before failure


def old_index(s):
    """v1: one signed index tuned to the HPC fault (FD001)."""
    return sum(sign * (s[k] - mu) / sd for k, (sign, mu, sd) in events.HEALTH_SENSORS.items()) / len(events.HEALTH_SENSORS)


def abs_index(s):
    """Alternative: mean absolute deviation (direction-agnostic)."""
    return sum(abs(s[k] - mu) / sd for k, (_, mu, sd) in events.HEALTH_SENSORS.items()) / len(events.HEALTH_SENSORS)


DESIGNS = {
    "v1 signed HPC index (old)": (old_index, 1.5, 2.5),
    "abs deviation": (abs_index, 2.0, 3.0),
    "v2 max(HPC, 0.4 x fan) (new)": (events.health_index, events.WARNING_AT, events.CRITICAL_AT),
}


def replay(rows, index, warn, crit, alpha=events.EWMA_ALPHA):
    by_unit = defaultdict(list)
    for r in rows:
        by_unit[r["unit"]].append(r)
    out = []
    for unit, rs in by_unit.items():
        rs.sort(key=lambda r: r["cycle"])
        last = rs[-1]["cycle"]
        ewma, w, c = None, None, None
        for r in rs:
            hi = index(r)
            ewma = hi if ewma is None else alpha * hi + (1 - alpha) * ewma
            if w is None and ewma >= warn:
                w = last - r["cycle"]
            if c is None and ewma >= crit:
                c = last - r["cycle"]
        out.append((unit, w, c))
    return out


def summarise(res):
    n = len(res)
    wl = [w for _, w, _ in res if w is not None]
    cl = [c for _, _, c in res if c is not None]
    p10 = lambda xs: sorted(xs)[max(0, len(xs) // 10 - 1)] if xs else None  # noqa: E731
    return {
        "engines": n,
        "warned": f"{len(wl)}/{n} ({100 * len(wl) // n}%)",
        "critical": f"{len(cl)}/{n} ({100 * len(cl) // n}%)",
        "warn lead median/p10": f"{st.median(wl):.0f} / {p10(wl)}" if wl else "-",
        "crit lead median/p10": f"{st.median(cl):.0f} / {p10(cl)}" if cl else "-",
        f"too early (>{EARLY})": sum(1 for w in wl if w > EARLY),
    }


def main():
    data = {f: load(f) for f in ("FD001", "FD003")}
    cols = None
    print(f"| Design | Fleet | Engines | Warned | Critical | Warning lead median / p10 | Critical lead median / p10 | Warnings > {EARLY} cycles early |")
    print("|---|---|---|---|---|---|---|---|")
    for name, (idx, warn, crit) in DESIGNS.items():
        for fleet, rows in data.items():
            s = summarise(replay(rows, idx, warn, crit))
            print(f"| {name} | {fleet} | " + " | ".join(str(v) for v in s.values()) + " |")


if __name__ == "__main__":
    main()
