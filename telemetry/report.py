"""Read the Delta tables (with delta-rs, no Spark needed) and print a health report of the pipeline."""
import json

import pyarrow.compute as pc
from deltalake import DeltaTable
from deltalake.exceptions import TableNotFoundError

from .config import DATA_DIR, LAKE, TABLES


def table(name, columns=None):
    try:
        return DeltaTable(str(TABLES[name])).to_pyarrow_table(columns=columns)
    except (TableNotFoundError, Exception):
        return None


def _pct(arr, q):
    return round(pc.quantile(arr, q=q)[0].as_py(), 2) if arr is not None and len(arr) else None


def report() -> dict:
    out = {}
    b = table("bronze", ["offset"])
    out["bronze_rows"] = b.num_rows if b is not None else 0
    s = table("silver", ["event_id", "event_time", "kafka_ts", "ingest_ts", "silver_ts"])
    if s is not None and s.num_rows:
        out["silver_rows"] = s.num_rows
        out["silver_distinct_event_ids"] = len(pc.unique(s["event_id"]))
        out["silver_duplicates"] = out["silver_rows"] - out["silver_distinct_event_ids"]
        ms = lambda a, b_: pc.divide(pc.cast(pc.subtract(s[a], s[b_]), "int64"), 1000.0)  # micros -> ms
        e2e = ms("silver_ts", "event_time")
        out["latency_event_to_silver_ms"] = {"p50": _pct(e2e, 0.5), "p95": _pct(e2e, 0.95), "p99": _pct(e2e, 0.99)}
        k2b = ms("ingest_ts", "kafka_ts")
        out["latency_kafka_to_bronze_ms"] = {"p50": _pct(k2b, 0.5), "p95": _pct(k2b, 0.95)}
    d = table("dlq", ["dlq_reason"])
    out["dead_letter"] = {r["dlq_reason"]: r["count"] for r in d.group_by("dlq_reason").aggregate([("dlq_reason", "count")]).rename_columns(["dlq_reason", "count"]).to_pylist()} if d is not None and d.num_rows else {}
    st = table("engine_state", ["alert_level"])
    if st is not None:
        lv = st["alert_level"].to_pylist()
        out["engines"] = {"total": len(lv), "ok": lv.count(0), "warning": lv.count(1), "critical": lv.count(2)}
    a = table("alerts", ["alert_id"])
    if a is not None:
        out["alerts"] = {"rows": a.num_rows, "distinct": len(pc.unique(a["alert_id"]))}
    f = table("fleet_minute", ["events"])
    if f is not None:
        out["fleet_minute_windows"] = f.num_rows
    prog = LAKE / "_metrics" / "progress.jsonl"
    if prog.exists():
        recs = [json.loads(x) for x in prog.read_text().splitlines() if x.strip()]
        peak = {}
        for r in recs:
            if r["rows"] and r["proc_rps"] > peak.get(r["query"], 0):
                peak[r["query"]] = r["proc_rps"]
        out["peak_processed_rows_per_s"] = peak
    print(json.dumps(out, indent=2))
    return out


def verify(run_id: str | None = None) -> bool:
    """Exactly-once proof: every valid event the producer sent is in silver exactly once, and nothing else is."""
    mdir = DATA_DIR / "manifests"
    runs = sorted(mdir.glob("*.ids"))
    if not runs:
        print("no producer manifests found: run the producer without --no-manifest first")
        return False
    files = [mdir / f"{run_id}.ids"] if run_id else runs
    expected = set()
    for p in files:
        expected |= set(x for x in p.read_text().split("\n") if x)
    s = table("silver", ["event_id"])
    got = s["event_id"].to_pylist() if s is not None else []
    got_set = set(got)
    missing, extra, dupes = expected - got_set, got_set - expected, len(got) - len(got_set)
    ok = not missing and not dupes and not extra
    print(json.dumps({"manifests": [p.name for p in files], "expected_valid_events": len(expected), "silver_rows": len(got),
                      "missing": len(missing), "unexpected": len(extra), "duplicates": dupes, "result": "PASS" if ok else "FAIL"}, indent=2))
    if missing:
        print("  e.g. missing:", sorted(missing)[:3], "(if the stream is still catching up, wait and re-run)")
    return ok
