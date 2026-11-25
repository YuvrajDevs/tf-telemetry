"""Start streaming layers in one Spark application and print throughput every N seconds."""
import json
import threading
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable

from ..config import LAKE, TABLES
from ..spark import get_spark
from . import bronze, gold, silver


def _wait_for(spark, path, timeout=600):
    t0 = time.time()
    while not DeltaTable.isDeltaTable(spark, str(path)):
        if time.time() - t0 > timeout:
            raise TimeoutError(f"{path} never appeared. Is the upstream layer running and receiving data?")
        print(f"  waiting for {path.name} to be created by the upstream query...")
        time.sleep(5)


def _monitor(spark, every):
    log = LAKE / "_metrics" / "progress.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    while True:
        time.sleep(every)
        lines = []
        for q in spark.streams.active:
            p = q.lastProgress
            if not p:
                continue
            rec = {"ts": datetime.now(timezone.utc).isoformat(), "query": q.name, "batch": p["batchId"],
                   "rows": p["numInputRows"], "in_rps": round(p.get("inputRowsPerSecond") or 0, 1),
                   "proc_rps": round(p.get("processedRowsPerSecond") or 0, 1),
                   "batch_ms": p["durationMs"].get("triggerExecution")}
            lines.append(rec)
            with log.open("a") as f:
                f.write(json.dumps(rec) + "\n")
        if lines:
            print("  " + " | ".join(f"{r['query']}: batch {r['batch']} rows {r['rows']} in {r['in_rps']}/s proc {r['proc_rps']}/s ({r['batch_ms']} ms)" for r in lines), flush=True)


def main(layers=("bronze", "silver", "gold"), monitor_every=15):
    spark = get_spark()
    queries = []
    if "bronze" in layers:
        queries.append(bronze.start(spark))
    if "silver" in layers:
        _wait_for(spark, TABLES["bronze"])
        queries += silver.start(spark)
    if "gold" in layers:
        _wait_for(spark, TABLES["silver"])
        queries += gold.start(spark)
    print(f"running: {[q.name for q in queries]}  ·  Spark UI: http://localhost:4040  ·  Ctrl+C to stop (safe: checkpoints resume)")
    threading.Thread(target=_monitor, args=(spark, monitor_every), daemon=True).start()
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("stopping queries...")
        for q in spark.streams.active:
            q.stop()
