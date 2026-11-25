"""Load test (Milestone T2). Measures what this pipeline sustains on the current machine.

Phase 1, catch-up: put a large backlog in Kafka first, then start the full pipeline (bronze, silver, dead letter,
gold) and time how fast it drains through silver. This is the maximum sustained processing rate.
Phase 2, steady state: with the pipeline running, produce at fixed rates and measure end-to-end latency
(event_time set by the producer -> row committed in silver) and whether the pipeline keeps up.

Prereqs: Kafka running (docker compose up -d), nothing else streaming.
    python scripts/loadtest.py --loops 10 --rates 1000 3000
Writes docs/loadtest-results.json and prints a markdown table for docs/capacity-plan.md.
"""
import argparse
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from telemetry.config import LAKE, MAX_OFFSETS_PER_TRIGGER, PARTITIONS, TABLES, TRIGGER  # noqa: E402

PY = sys.executable


def cli(*args, **kw):
    return subprocess.run([PY, "-m", "telemetry.cli", *args], cwd=ROOT, check=True, capture_output=True, text=True, **kw)


def silver_count(engine_suffix=None):
    from deltalake import DeltaTable
    p = TABLES["silver"]
    if not (p / "_delta_log").exists():
        return 0
    dt = DeltaTable(str(p))
    if engine_suffix is None:
        return sum(a["num_records"] for a in dt.get_add_actions(flatten=True).to_pylist())
    t = dt.to_pyarrow_table(columns=["engine_id"])
    import pyarrow.compute as pc
    return pc.sum(pc.ends_with(t["engine_id"], engine_suffix).cast("int64")).as_py() or 0


def produced(out: str) -> int:
    for line in out.splitlines():
        if '"messages_sent"' in line:
            return int(line.split(":")[1].strip(" ,"))
    raise RuntimeError("producer summary not found:\n" + out[-2000:])


def latency(loop_ids):
    from deltalake import DeltaTable
    import pyarrow.compute as pc
    t = DeltaTable(str(TABLES["silver"])).to_pyarrow_table(columns=["engine_id", "event_time", "silver_ts"])
    mask = None
    for n in loop_ids:
        m = pc.ends_with(t["engine_id"], f"-L{n}")
        mask = m if mask is None else pc.or_(mask, m)
    t = t.filter(mask)
    ms = pc.divide(pc.subtract(t["silver_ts"].cast("int64"), t["event_time"].cast("int64")), 1000).to_pylist()
    ms.sort()
    q = lambda p: round(ms[min(len(ms) - 1, int(p * len(ms)))]) if ms else None  # noqa: E731
    return {"rows": len(ms), "p50_ms": q(0.5), "p95_ms": q(0.95), "p99_ms": q(0.99)}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--loops", type=int, default=10, help="fleet replays in the backlog (~45k events each)")
    ap.add_argument("--rates", type=int, nargs="*", default=[1000, 3000])
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--layers", nargs="*", default=["bronze", "silver", "gold"], help="which layers to run")
    ap.add_argument("--out", default="docs/loadtest-results.json")
    a = ap.parse_args()

    shutil.rmtree(LAKE, ignore_errors=True)
    subprocess.run([PY, "-m", "telemetry.cli", "delete-topic"], cwd=ROOT, capture_output=True)
    time.sleep(3)
    cli("create-topic", "--partitions", str(PARTITIONS))

    # ---- phase 1: backlog
    t0 = time.time()
    out = cli("produce", "--rate", "0", "--loops", str(a.loops), "--start-loop", "100", "--no-manifest").stdout
    n = produced(out)
    prod_s = time.time() - t0
    print(f"backlog: {n:,} events produced in {prod_s:.1f}s ({n / prod_s:,.0f}/s)", flush=True)

    log = open(ROOT / "data" / "loadtest-stream.log", "w")
    stream = subprocess.Popen([PY, "-m", "telemetry.cli", "stream", *a.layers], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
    started = time.time()
    samples = []
    try:
        while True:
            c = silver_count()
            samples.append((time.time(), c))
            if c >= n:
                break
            if time.time() - started > 1800:
                raise TimeoutError("backlog did not drain in 30 minutes")
            time.sleep(2)
        done = time.time()
        first = next(ts for ts, c in samples if c > 0)
        # Wall-clock throughput including JVM start-up: conservative, and not fooled by a few huge batches.
        # (Measuring between 10% and 90% of the backlog is misleading here: with up to 200k rows per batch,
        # that window can fall between just two commits.)
        wall = n / (done - started)
        print(f"drained {n:,} events: first silver rows after {first - started:.0f}s, all after {done - started:.0f}s "
              f"(cold start included): {wall:,.0f} events/s end to end", flush=True)

        # ---- phase 2: steady state at fixed rates
        steady = []
        for i, rate in enumerate(a.rates):
            loop0 = 200 + i * 10
            loops = max(1, -(-rate * a.seconds // 44000))
            t1 = time.time()
            out = cli("produce", "--rate", str(rate), "--loops", str(loops), "--start-loop", str(loop0),
                      "--max-events", str(rate * a.seconds), "--no-manifest").stdout
            sent = produced(out)
            send_s = time.time() - t1
            ids = list(range(loop0, loop0 + loops))
            t2 = time.time()
            while sum(silver_count(f"-L{x}") for x in ids) < sent and time.time() - t2 < 600:
                time.sleep(2)
            lag_s = time.time() - t2
            lat = latency(ids)
            steady.append({"target_rate": rate, "sent": sent, "actual_rate": round(sent / send_s), "drain_after_send_s": round(lag_s, 1), **lat})
            print(f"steady {rate}/s: sent {sent:,} ({sent / send_s:,.0f}/s), silver caught up {lag_s:.0f}s after the producer stopped, "
                  f"latency p50 {lat['p50_ms']} ms p95 {lat['p95_ms']} ms", flush=True)
    finally:
        stream.terminate()
        try:
            stream.wait(30)
        except subprocess.TimeoutExpired:
            stream.kill()

    peaks = {}
    prog = LAKE / "_metrics" / "progress.jsonl"
    batch_ms = {}
    if prog.exists():
        for line in prog.read_text().splitlines():
            r = json.loads(line)
            if r["rows"]:
                peaks[r["query"]] = max(peaks.get(r["query"], 0), r["proc_rps"])
                batch_ms.setdefault(r["query"], []).append(r["batch_ms"])
    res = {
        "machine": {"cpus": os.cpu_count(), "ram_gb": round(os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30),
                    "python": platform.python_version(), "os": platform.system()},
        "settings": {"partitions": PARTITIONS, "trigger": TRIGGER, "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
                     "shuffle_partitions": os.environ.get("TT_SHUFFLE_PARTITIONS", "default"), "layers": a.layers},
        "backlog": {"events": n, "producer_events_per_s": round(n / prod_s), "first_rows_after_s": round(first - started),
                    "drain_s": round(done - started), "wall_clock_events_per_s": round(wall)},
        "steady": steady,
        "peak_processed_rows_per_s": peaks,
        "median_batch_ms": {q: sorted(v)[len(v) // 2] for q, v in batch_ms.items()},
    }
    (ROOT / a.out).write_text(json.dumps(res, indent=2))
    print(json.dumps(res, indent=2))


if __name__ == "__main__":
    main()
