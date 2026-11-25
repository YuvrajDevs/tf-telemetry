"""Command line entry point:  python -m telemetry.cli <command> [options]"""
import argparse
import shutil

from . import admin, cmapss
from .config import DATA_DIR, LAKE


def main(argv=None):
    p = argparse.ArgumentParser(prog="telemetry", description="Turbofan Telemetry Streaming Platform")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download", help="download the NASA C-MAPSS dataset into data/cmapss")
    t = sub.add_parser("create-topic", help="create the Kafka topic")
    t.add_argument("--partitions", type=int, default=6)
    sub.add_parser("describe-topic")
    sub.add_parser("delete-topic")
    pr = sub.add_parser("produce", help="replay engines into Kafka")
    pr.add_argument("--rate", type=float, default=500, help="events/second, 0 = as fast as possible")
    pr.add_argument("--engines", type=int, default=None, help="limit engines per subset")
    pr.add_argument("--loops", type=int, default=1, help="replay the fleet N times (new engine ids each loop)")
    pr.add_argument("--start-loop", type=int, default=0, help="first loop number; use a new value for each run so engine ids never repeat")
    pr.add_argument("--max-events", type=int, default=None)
    pr.add_argument("--dup-rate", type=float, default=0.0)
    pr.add_argument("--late-rate", type=float, default=0.0)
    pr.add_argument("--late-seconds", type=float, default=30.0)
    pr.add_argument("--bad-rate", type=float, default=0.0)
    pr.add_argument("--chaos", action="store_true", help="shortcut: dup 5%%, late 3%% (30 s), bad 1%%")
    pr.add_argument("--no-manifest", action="store_true", help="don't record event ids (use for big load tests)")
    st = sub.add_parser("stream", help="run streaming layers")
    st.add_argument("layers", nargs="*", choices=["bronze", "silver", "gold"], help="default: all three")
    sub.add_parser("report", help="print pipeline health: counts, duplicates, dead letters, latency, alerts")
    v = sub.add_parser("verify", help="exactly-once check: producer manifest vs silver")
    v.add_argument("--run", default=None)
    sub.add_parser("reset", help="delete the local lake (tables + checkpoints) and manifests")
    a = p.parse_args(argv)

    if a.cmd == "download":
        cmapss.download()
    elif a.cmd == "create-topic":
        admin.create_topic(a.partitions)
    elif a.cmd == "describe-topic":
        admin.describe()
    elif a.cmd == "delete-topic":
        admin.delete_topic()
    elif a.cmd == "produce":
        from . import producer
        dup, late, bad = (0.05, 0.03, 0.01) if a.chaos else (a.dup_rate, a.late_rate, a.bad_rate)
        producer.run(rate=a.rate, engines=a.engines, loops=a.loops, start_loop=a.start_loop, max_events=a.max_events, dup_rate=dup,
                     late_rate=late, late_seconds=a.late_seconds, bad_rate=bad, manifest=not a.no_manifest)
    elif a.cmd == "stream":
        from .jobs import run
        run.main(tuple(a.layers or ("bronze", "silver", "gold")))
    elif a.cmd == "report":
        from . import report
        report.report()
    elif a.cmd == "verify":
        from . import report
        raise SystemExit(0 if report.verify(a.run) else 1)
    elif a.cmd == "reset":
        shutil.rmtree(LAKE, ignore_errors=True)
        shutil.rmtree(DATA_DIR / "manifests", ignore_errors=True)
        print(f"removed {LAKE} and producer manifests (Kafka topic untouched: delete-topic + create-topic to clear it)")


if __name__ == "__main__":
    main()
