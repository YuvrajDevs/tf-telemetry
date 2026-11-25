# Build log: how this project was created, step by step

This is the complete story of how the Turbofan Telemetry Streaming Platform was built: every decision
(including the ones made by me, the project owner), the alternatives we considered, why we chose what we
did, and **every failure we hit along the way**, with the exact error message, how it was diagnosed, and
how it was fixed. Failures are numbered `F1`, `F2`, … so you can refer to them in interviews.

> Reading tip: the "Why" and "Lesson" lines are what interviewers care about. If you can explain those
> in your own words, you understand the project.

---

## Phase 0. Choosing what to build

**Goal.** Two portfolio projects that get me selected for a Data Engineer 1 role and show Data Engineer 2
depth: not just "a pipeline that runs once", but production concerns such as idempotency, failure recovery,
data quality, monitoring and scale.

| Decision | Options considered | Chosen | Why |
|---|---|---|---|
| How many projects | 1 big / 2 / 3 | **2** (my choice) | Depth beats breadth. Two projects I can defend for 30 minutes each. |
| Data domain | Data that mirrors my current employer's internal processes / public data | **Public data only** (my choice) | Anything resembling an employer's internal data in a public GitHub repo is a confidentiality risk. Public datasets are safe and verifiable. |
| Which project first | Batch data product / streaming platform | **Streaming: Turbofan telemetry** (my choice) | It covers the posting's "event-based sources", Kafka, Spark, IoT and "scale" questions, and real engine data is a strong story. |
| Accounts / cost | Cloud (Azure/AWS/Databricks) / local | **Everything local, free, no logins** | No accounts needed except GitHub. Docker runs Kafka; Spark runs on the laptop. Easy for any reviewer to reproduce. |

---

## Phase 1. Finding the data

**Requirement:** real engine sensor data, public, downloadable without an account.

| Option | Verdict |
|---|---|
| Random synthetic numbers | Rejected as the main source: no real story, easy to dismiss in an interview. Kept as an **offline fallback**. |
| NASA C-MAPSS on Kaggle | Rejected: Kaggle needs a login. |
| NASA open data portal link (`data.nasa.gov/.../CMAPSSData.zip`) | **Failed** (see F3). |
| NASA Prognostics Center (PHM) dataset mirror on S3 | **Chosen.** Official, public, no login, 12 MB. |

**What the data is.** NASA simulated turbofan (jet) engines with the C-MAPSS model. Each engine starts
healthy and runs until failure. One row = one operating cycle (≈ one flight): `unit, cycle, 3 operating
settings, 21 sensors`. Four subsets:

| Subset | Engines (train) | Operating conditions | Fault modes |
|---|---|---|---|
| FD001 | 100 | 1 | 1 (HPC degradation) |
| FD002 | 260 | 6 | 1 |
| FD003 | 100 | 1 | 2 (HPC + fan) |
| FD004 | 248 | 6 | 2 |

### F3. NASA portal link dead
- **Symptom:** `curl -I https://data.nasa.gov/docs/legacy/CMAPSSData.zip` → no response (HTTP `000`).
- **Diagnosis:** NASA reorganised its open-data portal; old links break.
- **Fix:** use the PHM S3 mirror (HTTP 200). `telemetry/cmapss.py` also has a **synthetic fallback** with
  the same shape, so the project still runs if the link moves again.
- **Lesson:** external sources move. Pin the source, document it, and design a fallback.

---

## Phase 2. Choosing the stack

| Layer | Options | Chosen | Why |
|---|---|---|---|
| Event transport | Kafka / Azure Event Hubs / AWS Kinesis / RabbitMQ | **Apache Kafka 3.9** | Industry standard, named in the job posting, runs locally free. Event Hubs exposes a Kafka-compatible API, so the skills transfer. |
| Kafka mode | ZooKeeper / **KRaft** | **KRaft** (single node) | ZooKeeper is removed in Kafka 4. KRaft = one container, simpler. |
| Kafka image | Bitnami / Confluent / official `apache/kafka` | **`apache/kafka:3.9.0`** | Official Apache image, pinned version. |
| Stream processing | Spark Structured Streaming / Flink / Kafka Streams | **Spark Structured Streaming** | Same engine as batch PySpark (named in the posting), micro-batch model is easy to reason about, Delta integration, very common on Databricks. Flink has lower latency but is a different skill set. |
| Spark version | 3.5 / **4.0** | **PySpark 4.0.1** | My environment has Python 3.13 and PySpark 3.5 doesn't officially support it (F4). Spark 4 also supports Java 17/21. |
| Storage format | Parquet files / **Delta Lake** / Iceberg | **Delta Lake 4.0** | ACID commits (no half-written files), MERGE for upserts, time travel, and Spark checkpoints + Delta give exactly-once. Iceberg would also work. Delta is the Databricks default. |
| Python Kafka client | kafka-python / **confluent-kafka** | **confluent-kafka 2.8** | Built on librdkafka (C): much faster, supports the idempotent producer. |
| Reading tables outside Spark | Spark / **delta-rs** (`deltalake`) | **delta-rs** | The report, verify and dashboard read Delta tables in milliseconds without starting a JVM. |
| Dashboard | Grafana / Superset / **Streamlit** | **Streamlit** | Pure Python, one file, enough for a demo UI. |
| Where Spark runs | Cluster / **`local[*]`** | **local[*]** | Free, reproducible on a laptop. The same code runs on Databricks/EMR by changing the master and paths. |

### F4. Python 3.13 vs PySpark 3.5
- **Problem:** PySpark 3.5 supports Python up to 3.12; my environment's Python is 3.13.
- **Fix:** use **PySpark 4.0.1 + delta-spark 4.0.0** (a matching pair: Delta releases target a Spark version).
  The Docker image uses Python 3.12 to be safe on every machine.
- **Lesson:** always check the compatibility matrix (Spark ↔ Delta ↔ Python ↔ Java ↔ Scala 2.13 jars).

---

## Phase 3. Exploring the data before writing pipelines

I computed, per sensor, the mean in each engine's **first cycle vs last cycle** on FD001:

| Sensor | Healthy → failure | Meaning |
|---|---|---|
| s1, s5, s6, s10, s16, s18, s19 | constant | No information: excluded from the health index |
| s2, s3, s4, s11, s15, s17 | **rise** (e.g. s11: 47.34 → 48.18, s4: 1402 → 1431) | Temperatures/pressures rising with wear |
| s7, s12, s20, s21 | **fall** (e.g. s12: 521.9 → 519.7) | Pressure/flow measures falling with wear |

Engines live **128-362 cycles** (average 206).

**Decision: use FD001 + FD003 only.** They run at one operating condition, so sensor drift = wear.
FD002/FD004 mix six operating conditions; the same sensor value means different things at different
altitudes, so they would need per-condition normalisation first (a possible extension).

---

## Phase 4. Designing the health index and calibrating alerts

**Health index** = average signed z-score of the 10 drifting sensors, using the mean/std of every engine's
first 30 (healthy) cycles as the baseline. About 0 when healthy, rising with wear.

**Smoothing:** a single reading is noisy, so the state keeps an **EWMA** (exponentially weighted moving
average): `ewma = α·hi + (1−α)·ewma_prev`. EWMA needs only one number of state per engine, which is ideal
for streaming (a rolling window would need to store the last N readings).

**Calibration:** I replayed all 100 FD001 engines offline and measured, for different α and thresholds,
how many cycles before failure each alert fires (the "lead time"):

| α | WARNING / CRITICAL at | WARNING lead (median, p10) | CRITICAL lead (median, p10) | Engines never critical |
|---|---|---|---|---|
| 0.1 | 1.5 / 2.5 | 41, 22 | 15, 8 | 0 |
| 0.1 | 2.0 / 3.0 | 25, 15 | 6, 2 | 4 |
| **0.2** | **1.5 / 2.5** | **44, 27** | **19, 12** | **0** |
| 0.2 | 2.0 / 3.0 | 30, 19 | 10, 6 | 0 |
| 0.2 | 1.0 / 2.0 | 68, 38 | 30, 19 | 0 |

**Chosen: α = 0.2, WARNING ≥ 1.5, CRITICAL ≥ 2.5.** Every engine caught, warning ~44 cycles ahead
(enough to plan maintenance), critical ~19 ahead. Lower thresholds warn earlier but would raise more
false alarms on healthy engines.

**Levels only escalate** (OK → WARNING → CRITICAL) until maintenance; an alert is raised once per level.

---

## Phase 5. The event contract

```json
{"schema_version": 1, "event_id": "6c1…", "fleet": "FD001", "engine_id": "FD001-007",
 "cycle": 113, "event_time": "2026-11-25T14:56:44.621Z",
 "op_settings": [0.0007, -0.0004, 100.0], "sensors": {"s1": 518.67, "...": "...", "s21": 23.419}}
```

| Choice | Why |
|---|---|
| `event_id = uuid5(engine_id:cycle)`, **deterministic** | The same reading always has the same id, so duplicates are detectable anywhere downstream. A random UUID per send would make a re-sent event look new. |
| Kafka message **key = engine_id** | Same key → same partition → events of one engine stay **in order**. |
| `event_time` set by the producer (UTC) | Processing uses **event time** (when it happened), not arrival time, so late data is handled correctly. |
| `schema_version` | Allows schema evolution without breaking consumers. |

---

## Phase 6. The producer (replaying the fleet)

- **Interleaving:** each "tick", every active engine emits its next cycle, so 200 engines stream in
  parallel like a real fleet, instead of engine 1's entire life followed by engine 2's.
- **Rate control:** `--rate` events/second (0 = as fast as possible).
- **Producer settings:** `acks=all` (wait for replicas), `enable.idempotence=true` (broker discards the
  producer's own retries), `linger.ms=20` + `lz4` compression (batching = throughput).
- **Chaos injection** to test the pipeline's guarantees:
  - `--dup-rate` re-sends events (simulates at-least-once delivery)
  - `--late-rate/--late-seconds` holds events back, then sends them (late arrivals)
  - `--bad-rate` sends truncated JSON, missing fields, or impossible values (s11 = −1)
  - `--chaos` = 5% duplicates, 3% late by 30 s, 1% bad
- **Manifest:** every run writes the ids of all valid events to `data/manifests/<run>.ids`. This is the
  ground truth that `verify` compares against: it's how exactly-once is **proven**, not assumed.
- **`--start-loop`** gives each run fresh engine ids (see F13).

---

## Phase 7. Bronze: Kafka → Delta, as received

- Stores the raw message plus Kafka coordinates (`topic, partition, offset, kafka_ts`) and `ingest_ts`.
- **Nothing is parsed or dropped**, so bronze is the replayable record. A bug in silver can be fixed and silver rebuilt from bronze.
- `maxOffsetsPerTrigger` caps each micro-batch (**backpressure**): after an outage the job catches up in controlled steps instead of trying to swallow hours of backlog at once.
- **Checkpoint** (offsets + commit log) + Delta's atomic commits = **exactly-once into bronze**.
- Partitioned by `ingest_date`.

---

## Phase 8. Silver: parse, validate, deduplicate, dead-letter

- `from_json` with an explicit schema. Each row gets a `dlq_reason`: `MALFORMED_JSON`, `MISSING_FIELD`,
  `OUT_OF_RANGE`, or NULL if valid. The same rules exist in pure Python (`events.validate`) for unit tests.
- **Two queries from the same bronze stream:** `silver` (valid) and `dead_letter` (invalid with raw payload
  and reason). Bad data is never silently dropped and never crashes the stream. A poison message is just
  another dead-letter row.
- **Deduplication:** `withWatermark("event_time", "10 minutes").dropDuplicatesWithinWatermark(["event_id"])`.
  Spark remembers event ids for the watermark window (in a RocksDB state store), then forgets them, so
  memory stays bounded. Trade-off: a duplicate arriving **more than 10 minutes later** would not be caught
  (see F13 and Milestone T4).
- `health_index` computed here as a column (deterministic, stateless).

---

## Phase 9. Gold: engine state, alerts, fleet health

**Engine current state + alerts** (`foreachBatch`):
1. Read the micro-batch of silver events.
2. Join each engine's events with its **previous state** from the `engine_current_state` Delta table.
3. `groupBy("engine_id").applyInPandas(...)`: for each engine, apply events **in cycle order** using the same
   pure-Python function as the unit tests (`events.advance_state`): update EWMA, escalate the level, emit alerts.
4. **MERGE** alerts on `alert_id = engine:LEVEL` (insert only if new) and **MERGE** state on `engine_id`
   (update only if `last_cycle` moved forward).

Why this is **idempotent**: if Spark crashes after writing but before checkpointing, it replays the same
batch. Events at or before `last_cycle` are ignored, and existing alert ids aren't re-inserted. Replays
change nothing.

**Fleet health per minute:** 1-minute event-time windows per fleet with a 2-minute watermark, `append`
mode. A window is written once, when it is final (see F11).

---

## Phase 10. Testing strategy

| Level | What | How |
|---|---|---|
| Unit | event ids, validation rules, health index, state machine (escalation, idempotent replay, out-of-order) | `pytest tests/`: 10 tests, run in 0.1 s, no Spark/Kafka needed |
| End-to-end | exactly-once under duplicates, late data, bad data **and a crash** | `scripts/demo.sh`: produce with chaos, `kill -9` the stream mid-run, restart, `verify` compares the manifest with silver |
| Health | counts, duplicates, dead-letter reasons, latency percentiles, throughput | `python -m telemetry.cli report` |

---

## Failure log (everything that went wrong, in order)

### F1. Docker daemon not running
- **Error:** `failed to connect to the docker API at unix:///var/run/docker.sock ... no such file or directory`
- **Cause:** the Docker CLI was installed but the daemon (the background service) wasn't started.
- **Fix:** start the daemon (on a laptop: open Docker Desktop).
- **Lesson:** "docker: command found" ≠ "Docker running". `docker info` shows whether the daemon is up.

### F2. Docker Hub rate limit
- **Error:** `429 Too Many Requests` when pulling `provectuslabs/kafka-ui` and `python:3.12-slim`.
- **Cause:** Docker Hub limits anonymous pulls per IP.
- **Fix:** the Kafka image was already pulled, so the pipeline was tested with native Python + Kafka in
  Docker. On your laptop this is rarely hit. If it is, wait, or sign in with a free Docker account (optional).
- **Lesson:** CI systems pulling public images hit this too. Companies mirror images in a private registry.

### F3. NASA download link dead
See Phase 1.

### F4. Python 3.13 not supported by PySpark 3.5
See Phase 2.

### F5. Topic creation silently failed
- **Error:** `KafkaError{code=_DESTROY,val=-197,str="Handle is terminating: Local: Broker handle destroyed"}`,
  then `engine-telemetry does not exist`.
- **Cause:** `client().create_topics(...)` created an `AdminClient`, started an async request and returned a
  future, but nothing kept a reference to the client, so Python garbage-collected it before the request finished.
- **Fix:** keep the client in a variable until `future.result()` returns.
- **Lesson:** async clients must outlive their futures.

### F6. CLI rejected its own default
- **Error:** `argument layers: invalid choice: "['bronze', 'silver', 'gold']"`
- **Cause:** argparse with `nargs="*"`, `choices=[...]` and a **list** default validates the whole list as one choice.
- **Fix:** no default in argparse; apply the default in code (`a.layers or ("bronze", "silver", "gold")`).

### F7. "Current batch is falling behind" warnings (not an error)
- **Message:** `ProcessingTimeExecutor: Current batch is falling behind. The trigger interval is 5000 milliseconds, but spent 9973 milliseconds`
- **Meaning:** the first micro-batches took longer than the 5 s trigger (JVM warm-up, loading jars, creating tables).
  It is only a problem if it persists under normal load. Then you'd increase the trigger interval,
  scale out, or reduce the per-batch cap.

### F8. "Server access error … repos.spark-packages.org … 403" (not an error)
- **Meaning:** when Spark downloads the Kafka/Delta jars (Ivy), it tries several repositories. One was blocked,
  Maven Central worked, and all 14 jars resolved. Harmless noise.

### F9. Python workers crashed: `ModuleNotFoundError: No module named 'pyarrow'`
- **Symptom:** after the first wave, `verify` → **FAIL: 1,329 events missing** (all of them the late events).
  `engine_current_state` stayed empty.
- **Diagnosis:** the log showed `PythonException … No module named 'pyarrow'` in the gold query. Spark
  starts **separate Python worker processes** for pandas UDFs, and they used the system Python (without
  pyarrow), not the project's virtual environment.
- **Consequence:** the gold query died, and because the app waits on `awaitAnyTermination()`, the whole app
  stopped, including bronze. That's why the late events, sent 30 s later, never made it in.
- **Fix:** set `PYSPARK_PYTHON` and `PYSPARK_DRIVER_PYTHON` to `sys.executable` in `get_spark()`.
- **Recovery (the good part):** I restarted the app **without deleting anything**. The checkpoints resumed
  each query exactly where it stopped. `verify` → **PASS: 44,913 expected, 44,913 in silver, 0 missing, 0
  duplicates, 0 unexpected**, even though about 2,200 duplicates had been sent.
- **Lessons:** (1) driver and worker Python must match (on clusters: same image/env on all nodes);
  (2) one failing query stopping the app is **fail-fast**: good for correctness, but in production you'd run
  layers as separate jobs so a gold bug can't stop ingestion; (3) checkpoints make crash recovery boring,
  which is the goal.

### F10. Alerts silently missing (the subtle one)
- **Symptom:** `engine_current_state` showed **156 CRITICAL engines**, but `engine_alerts` had **0 rows**. No error anywhere.
- **Diagnosis, step by step:**
  1. Delta history (`DeltaTable(...).history()`) showed the state table had MERGE commits, but the alerts
     table had **no MERGE commit at all**, so the alerts DataFrame was empty when it was written.
  2. Ran the pure-Python fold on one engine's real events: it returned WARNING and CRITICAL alerts. The logic was fine.
  3. Ran the same thing through Spark `applyInPandas` in a batch job: alerts appeared. So the bug was in how
     the streaming batch was **executed**, not in the logic.
  4. The code wrote **state first**, then alerts. Spark DataFrames are **lazy**: `out` is a recipe that
     includes "read the previous state table". `persist()` asks Spark to cache it but doesn't guarantee
     the cache is used, so when the alerts were computed, Spark could **recompute** `out` from scratch and
     re-read a state table that **had just been updated**. Every event now looked "already applied",
     so no alerts were produced.
- **Fix:** `out = ....localCheckpoint(eager=True)` computes the batch result once and cuts the lineage
  back to the state table. Write alerts first, then state.
- **Result:** 156 WARNING and 156 CRITICAL alerts, one per engine per level.
- **Lesson:** in Spark, **never read and write the same table within one lazy plan** without materialising
  first. `cache()` is a performance hint, not a correctness guarantee. This is the kind of bug that passes
  unit tests and only shows up end to end, which is why the verification step exists.

### F11. Fleet-minute table empty (correct behaviour)
- **Symptom:** `fleet_health_minute` had 0 rows after the first wave.
- **Explanation:** in **append** mode a window is written only once it is **final**, meaning the watermark
  (max event time seen − 2 minutes) has passed the window's end. All first-wave events happened within
  about 1 minute and then the stream went quiet, so the watermark never moved past them.
- **Fix:** none needed. A second, slower wave (`demo.sh` step 6) moves the watermark forward and the windows
  appear. In a real fleet, data never stops, so this resolves itself.
- **Lesson:** this is the exact trade-off watermarks encode: completeness (wait for late data) vs latency.
  `update` mode would show partial windows immediately, at the cost of rewriting them.

### F12. p99 latency of ~400 s
- **Explanation:** p50 ≈ 5.8 s and p95 ≈ 6.4 s, but p99 included the 3% of events deliberately delayed by
  30 s **plus** the time the app was down during F9. Percentiles tell the story: always report p50/p95/p99,
  never just an average.

### F13. Nearly re-sent old event ids
- **What happened:** a second producer run would have replayed engines with the **same ids** as the first
  run 15 minutes earlier. Silver only de-duplicates inside the 10-minute watermark, so those would have
  landed as duplicates.
- **Fix:** `--start-loop` gives every run fresh engine ids (`FD001-007-L1`).
- **Lesson / design trade-off:** watermark-based dedup is bounded in time by design. For **global**
  exactly-once you'd MERGE into silver on `event_id` (more expensive) or keep a longer watermark (more
  state). That's Milestone **T4**.

### F14. 44 of 100 FD003 engines never alerted
- **Observation:** all 100 FD001 engines alerted; only 56 of 100 FD003 engines did.
- **Diagnosis:** the missed engines have FD003's **second fault mode** (fan degradation). For them,
  s7, s12, s20 and s21 move **up** (+14.9, +14.4, +0.6, +0.4 vs baseline) while the health index expects
  them to move **down**, so they cancel out the rising sensors.
- **Status:** left as **Milestone T1** (yours): make the index fault-mode aware and measure the improvement.
- **Lesson:** a model calibrated on one population (FD001) silently under-performs on another (FD003).
  Monitor detection quality per segment, not just overall.

### F15. Producer crashed: `ValueError: sleep length must be non-negative`
- **When:** the first run of `scripts/demo.sh` (producing at 3,000 events/s).
- **Code before:**
  ```python
  if target > time.time():
      time.sleep(target - time.time())   # clock read a SECOND time
  ```
- **Cause:** a check-then-act race. Between the check and the sleep, time moved past `target`, so the
  computed sleep was negative. At low rates this never happens; at high rates it eventually does.
- **Fix:** read the clock once: `delay = target - time.time(); if delay > 0: time.sleep(delay)`.
- **Side effect worth noting:** the crash happened before `producer.flush()`, so 51 queued messages were
  never sent and no manifest was written. Always flush on shutdown (a `try/finally` is a good extension).
- **Lesson:** load tests find bugs that functional tests don't. Timing code must tolerate the clock moving.

---

## Final demo run (clean, scripted, with a crash)

`scripts/demo.sh` from a clean slate (topic recreated, lake deleted):

1. Started the streaming app (5 queries: bronze, silver, dead_letter, engine_state, fleet_health_minute).
2. Wave 1: whole fleet at 3,000 events/s with chaos: 47,578 messages = 44,913 valid unique + 2,227 duplicates + 1,386 late (30 s) + 438 bad.
3. **12 seconds in, `kill -9` of the Spark app** (no graceful shutdown), then restart 5 s later.
4. Wave 2: 40 new engines (`--start-loop 1`) at 100 events/s: 9,874 messages.
5. `verify`: **54,787 expected, 54,787 in silver, 0 missing, 0 duplicates, 0 unexpected → PASS.**
6. `report`: 438 dead letters split exactly by reason; 345 alerts, all distinct; fleet windows closed once the
   watermark passed them (F11 confirmed).

**About latency:** p50 event→silver was ~33 s in this run because it includes the crash, restart and catch-up
with 5 queries sharing 4 CPU cores. It is not a steady-state figure. Steady-state throughput and latency are
measured in Milestone T2 (load test) and recorded in `docs/capacity-plan.md`.
