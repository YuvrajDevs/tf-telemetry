"""Build site/data.json for the project page (GitHub Pages) from the real NASA C-MAPSS data.

For every FD001/FD003 engine: the smoothed health index per cycle under v1 (HPC-only) and v2 (fault-mode aware),
the dominant fault mode, and the cycles where WARNING / CRITICAL fire. Same functions as the streaming job.

    python scripts/build_site.py
"""
import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import calibrate  # noqa: E402
from telemetry import events  # noqa: E402
from telemetry.cmapss import load  # noqa: E402

A = events.EWMA_ALPHA


def series(rs, index):
    ewma, out, w, c = None, [], None, None
    for r in rs:
        hi = index(r)
        ewma = hi if ewma is None else A * hi + (1 - A) * ewma
        out.append(round(ewma, 2))
        if w is None and ewma >= events.WARNING_AT:
            w = r["cycle"]
        if c is None and ewma >= events.CRITICAL_AT:
            c = r["cycle"]
    return out, w, c


def main():
    engines = []
    table = []
    for fleet in ("FD001", "FD003"):
        rows = load(fleet)
        by = defaultdict(list)
        for r in rows:
            by[r["unit"]].append(r)
        for unit, rs in sorted(by.items()):
            rs.sort(key=lambda r: r["cycle"])
            v1, w1, c1 = series(rs, calibrate.old_index)
            v2, w2, c2 = series(rs, events.health_index)
            tail = rs[-10:]
            fan_votes = sum(events.fault_mode(r) == "FAN" for r in tail)
            engines.append({"id": f"{fleet}-{unit:03d}", "fleet": fleet, "life": rs[-1]["cycle"],
                            "mode": "FAN" if fan_votes > 5 else "HPC",
                            "v1": v1, "v2": v2, "w1": w1, "c1": c1, "w2": w2, "c2": c2})
        for name, (idx, warn, crit) in calibrate.DESIGNS.items():
            table.append({"design": name, "fleet": fleet, **calibrate.summarise(calibrate.replay(rows, idx, warn, crit))})
    out = ROOT / "site" / "data.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps({"warn": events.WARNING_AT, "crit": events.CRITICAL_AT, "alpha": A,
                               "engines": engines, "calibration": table}, separators=(",", ":")))
    print(f"wrote {out} ({out.stat().st_size // 1024} KB, {len(engines)} engines)")


if __name__ == "__main__":
    main()
