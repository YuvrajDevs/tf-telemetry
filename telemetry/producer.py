"""Replay C-MAPSS engines into Kafka as a live stream, with optional chaos.

Every "tick", each active engine emits its next cycle, so many engines stream in
parallel like a real fleet. The message key is engine_id, so all events of one engine
land in the same partition and stay in order.

Chaos (for testing the pipeline's guarantees):
  --dup-rate    resend some events (at-least-once delivery duplicates)
  --late-rate   hold some events back for --late-seconds before sending (late arrivals)
  --bad-rate    send malformed / incomplete / out-of-range events (must go to the dead-letter table)
"""
import heapq
import json
import random
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone

from confluent_kafka import Producer

from . import cmapss
from .config import BOOTSTRAP, DATA_DIR, TOPIC
from .events import make_event, to_bytes


def fleet_stream(subsets, loops, engines_limit=None, start_loop=0):
    """Yield (subset, row, loop) interleaved across engines: one cycle per engine per tick.
    Each loop replays the fleet under new engine ids (FD001-007-L1, -L2, ...), so a new run never
    re-sends an old event_id (silver only de-duplicates inside the watermark window)."""
    for loop in range(start_loop, start_loop + loops):
        iters = []
        for sub in subsets:
            by_unit = defaultdict(list)
            for r in cmapss.load(sub):
                by_unit[r["unit"]].append(r)
            units = sorted(by_unit)[:engines_limit] if engines_limit else sorted(by_unit)
            iters += [(sub, iter(sorted(by_unit[u], key=lambda r: r["cycle"]))) for u in units]
        while iters:
            alive = []
            for sub, it in iters:
                row = next(it, None)
                if row is not None:
                    yield sub, row, loop
                    alive.append((sub, it))
            iters = alive


def _bad_event(rnd, good: dict) -> tuple[bytes, str]:
    kind = rnd.choice(["MALFORMED_JSON", "MISSING_FIELD", "OUT_OF_RANGE"])
    if kind == "MALFORMED_JSON":
        return to_bytes(good)[: rnd.randint(5, 60)], kind          # truncated message
    bad = json.loads(to_bytes(good))
    bad["event_id"] = str(uuid.uuid4())                            # never counts as a valid event
    if kind == "MISSING_FIELD":
        bad.pop(rnd.choice(["engine_id", "cycle", "event_time"]))
    else:
        bad["sensors"]["s11"] = -1.0                               # physically impossible pressure
    return to_bytes(bad), kind


def run(rate: float = 500, subsets=cmapss.DEFAULT_SUBSETS, loops: int = 1, engines: int | None = None, start_loop: int = 0,
        dup_rate: float = 0.0, late_rate: float = 0.0, late_seconds: float = 30.0, bad_rate: float = 0.0,
        max_events: int | None = None, manifest: bool = True, seed: int = 42) -> dict:
    rnd = random.Random(seed)
    run_id = datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%SZ")
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "acks": "all",                 # wait for all in-sync replicas
        "enable.idempotence": True,    # broker drops producer retries: no duplicates from the producer itself
        "linger.ms": 20,               # small batching delay -> much higher throughput
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
    })
    errors = Counter()

    def on_delivery(err, msg):
        if err:
            errors[str(err)] += 1

    stats = Counter()
    valid_ids = []
    delayed = []  # heap of (send_at, seq, key, payload)
    seq = 0
    start = time.time()
    sent = 0

    def send(key: str, payload: bytes):
        nonlocal sent
        while True:
            try:
                producer.produce(TOPIC, key=key, value=payload, on_delivery=on_delivery)
                break
            except BufferError:          # local queue full: let it drain (backpressure)
                producer.poll(0.05)
        sent += 1
        producer.poll(0)

    print(f"[{run_id}] producing to {TOPIC} @ {BOOTSTRAP} rate={rate or 'max'}/s dup={dup_rate} late={late_rate} bad={bad_rate}")
    for sub, row, loop in fleet_stream(subsets, loops, engines, start_loop):
        if max_events and stats["events"] >= max_events:
            break
        now = time.time()
        while delayed and delayed[0][0] <= now:            # release late events whose delay is over
            _, _, k, p = heapq.heappop(delayed)
            send(k, p)
        ev = make_event(sub, row, loop)
        payload = to_bytes(ev)
        stats["events"] += 1
        if rnd.random() < bad_rate:
            bad, kind = _bad_event(rnd, ev)
            send(ev["engine_id"], bad)
            stats[f"bad_{kind}"] += 1
            continue
        valid_ids.append(ev["event_id"]) if manifest else None
        stats["valid_unique"] += 1
        if rnd.random() < late_rate:
            seq += 1
            heapq.heappush(delayed, (now + late_seconds, seq, ev["engine_id"], payload))
            stats["late"] += 1
        else:
            send(ev["engine_id"], payload)
        if rnd.random() < dup_rate:
            send(ev["engine_id"], payload)
            stats["duplicates_sent"] += 1
        if rate:
            delay = start + stats["events"] / rate - time.time()   # read the clock ONCE (see build-log F15)
            if delay > 0:
                time.sleep(delay)
        if stats["events"] % 20000 == 0:
            el = time.time() - start
            print(f"  {stats['events']:,} events in {el:,.1f}s ({stats['events'] / el:,.0f}/s)")
    while delayed:
        wait = delayed[0][0] - time.time()
        if wait > 0:
            time.sleep(min(wait, 1))
            continue
        _, _, k, p = heapq.heappop(delayed)
        send(k, p)
    producer.flush(60)
    elapsed = time.time() - start
    summary = {"run_id": run_id, "topic": TOPIC, "messages_sent": sent, "elapsed_s": round(elapsed, 2),
               "throughput_msgs_per_s": round(sent / elapsed, 1) if elapsed else None, "delivery_errors": dict(errors), **stats}
    if manifest:
        mdir = DATA_DIR / "manifests"
        mdir.mkdir(parents=True, exist_ok=True)
        (mdir / f"{run_id}.ids").write_text("\n".join(valid_ids))
        (mdir / f"{run_id}.json").write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return summary
