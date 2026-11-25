# Milestones: understand it, run it, break it, extend it

The pipeline works end to end. To **own** it (and defend it in interviews), go through these in order.
Each milestone has: what to read, what to run, questions you must be able to answer out loud, and for
the second half, **code you write yourself**.

---

## Part A: understand and run (days 1-3)

### M0. Set up (30 min)
Follow the README quick start. Done when `pytest` passes and `docker compose up -d` shows Kafka healthy.
**Explain:** what is a virtual environment? What does Docker Compose do here? Why do you need Java for PySpark?

### M1. The data
Read `telemetry/cmapss.py` and `docs/build-log.md` phases 1, 3 and 4. Open `data/cmapss/train_FD001.txt`.
**Explain:** what is one row? Why only FD001/FD003? Which sensors carry information and how did you find out?
What is the health index and why EWMA instead of a raw value?

### M2. Events and Kafka
Read `telemetry/events.py` and `telemetry/producer.py`. Run `python -m telemetry.cli produce --rate 50 --engines 2`
and watch messages arrive in Kafka UI (http://localhost:8080).
**Explain:** why is `event_id` deterministic? Why is the key `engine_id`? What do `acks=all` and
`enable.idempotence` protect against, and what do they **not** protect against?

### M3. Bronze and silver
Read `telemetry/jobs/bronze.py` and `silver.py`. Run `python -m telemetry.cli stream`, produce with `--chaos`,
then `report`. Open the dead-letter table in the dashboard.
**Explain:** why parse nothing in bronze? Why a separate dead-letter query? What does the watermark do for
deduplication, and what is the trade-off of 10 minutes vs 2 hours?

### M4. Gold and the F10 bug
Read `telemetry/jobs/gold.py` and build-log F10.
**Explain:** why `foreachBatch`? Why is the gold step idempotent? Explain the lazy-recomputation bug and the fix
**without notes**.

### M5. Break it on purpose
Run `scripts/demo.sh`. Then, by hand: kill the stream mid-wave and restart; send 5,000 bad events
(`--bad-rate 1 --max-events 5000`); stop the stream for 10 minutes while producing, then restart and watch it catch up
in capped batches (Spark UI, http://localhost:4040).
**Explain:** what exactly is in a checkpoint folder? What would happen if you deleted it?

---

## Part B: extend it (your code, your story)

### T1. Fault-mode aware detection ✅ done (build-log Phase 11) — now make it yours ★
FD003 detection went from 56% to 100% by scoring the HPC and fan fault patterns separately and taking the worse
(fan weighted 0.4), with the weight tuned on odd engines and checked on even ones.
**Do:** run `python scripts/calibrate.py` and read every row. Change `FAN_WEIGHT` to 1.0 and 0.2 and watch lead times and
false alarms move. Open `site/index.html` (via `python -m http.server -d site`) and compare v1/v2 on a few FAN engines.
**Explain without notes:** why the fan engines cancelled out; why not just use absolute deviation; why tune on odd
engines and test on even; why there is a Spark/Python parity test; what you would do with a third fault type.

### T2. Load test and capacity plan ✅ done (`scripts/loadtest.py`, `docs/capacity-plan.md`) — re-run it on your machine ★
**Do:** run `python scripts/loadtest.py` on your own laptop and replace the numbers in the capacity plan with yours.
Then try 12 partitions vs 6, `TT_MAX_OFFSETS=50000`, `TT_TRIGGER="1 second"`, and note what changes.
**Explain:** what is the bottleneck (look at median batch time per query)? Why does latency at a low rate mostly
equal the trigger interval times the number of layers? How many cores for 10,000 engines?

### T3. Schema evolution
Add `schema_version: 2` with a new field (e.g. `ambient_temp_c`). Old and new events must both flow; silver keeps the
new column nullable. **Explain** backward vs forward compatibility and why a schema registry exists.

### T4. Global deduplication
Replace (or complement) watermark dedupe with a `foreachBatch` MERGE into silver on `event_id`, insert-only.
Measure the cost difference at 1M rows. **Explain** when each approach is right.

### T5. Maintenance resets
Add a `maintenance` event type that resets an engine's state and alert level. Think about ordering: what if
maintenance arrives before the last pre-maintenance reading?

### T6. Small files and compaction
After a load test, count files in silver (`DeltaTable(...).files()`), run `OPTIMIZE` (Spark SQL) and compare query time.
**Explain** why streaming creates small files and how to schedule compaction.

### T7. Run it on Databricks Free Edition (optional)
Same code, cloud storage paths, a job instead of `local[*]`. Screenshot the Spark UI and the Delta table history.

---

## Interview questions this project prepares you for

1. *Walk me through your streaming architecture.* (architecture.md diagram, in 2 minutes)
2. *How do you guarantee exactly-once?* (checkpoints + Delta atomic commits + deterministic ids + idempotent MERGE, **and** how you proved it: manifest vs silver after `kill -9`)
3. *What happens when a malformed message arrives?* (dead-letter table, with reason, never blocks the stream)
4. *How do you handle late data?* (event time + watermark; trade-off completeness vs state and latency; F11)
5. *How would you scale this to 10,000 engines / 1 billion events a day?* (T2 numbers, partitions, consumers, autoscaling, compaction)
6. *Tell me about a hard bug.* (F10, lazy recomputation; or F9, Python environment on workers)
7. *Why Spark and not Flink? Why Delta and not plain Parquet?* (ADRs)
8. *How do you know your alerts are good?* (lead-time calibration table; F14 segment analysis; T1)
9. *What would you change for production?* (separate jobs per layer, schema registry, alert routing, monitoring of lag/freshness, multi-broker Kafka with replication 3, CI/CD)
10. *What is a watermark? A checkpoint? A consumer group? Backpressure?* (each has a concrete place in this code)
