# Turbofan Telemetry: resume and interview kit

## Resume entry (pick 3-4 bullets)

**Turbofan Telemetry: real-time predictive-maintenance pipeline** · Kafka, Spark Structured Streaming, Delta Lake, Python
· [project page](https://yuvrajdevs.github.io/tf-telemetry/) · [code](https://github.com/YuvrajDevs/tf-telemetry)

- Built a streaming pipeline that replays NASA's C-MAPSS jet-engine fleet (200 engines, 21 sensors) through **Kafka**
  into **Delta Lake bronze/silver/gold** tables with **Spark Structured Streaming**, raising maintenance alerts a median
  **44 cycles before failure** for **100% of engines**.
- Guaranteed **exactly-once** delivery with deterministic event ids, checkpoints, watermark deduplication and
  idempotent `MERGE`; **verified 54,787/54,787 events with 0 loss and 0 duplicates after a `kill -9` mid-stream**
  while injecting duplicates, late and malformed data.
- Routed every bad record to a **dead-letter table** with a reason code (438/438 captured) so one broken message never
  stops the stream.
- Found that the alert model **missed 44% of one fleet** (a second fault type cancelled out the score); built a
  fault-mode-aware health index, tuned on half the engines and validated on the other half, taking detection
  **from 56% to 100%** with no extra false alarms, and labelled each alert HPC/FAN for routing.
- **Load-tested** the pipeline on 4 cores (keeps up at 3,000 events/s; ~30,000 events/s ingest), used Delta commit
  history to identify the stateful gold step as the bottleneck (removing its CPU contention cut p95 latency from
  24.6 s to 10.8 s), and wrote a capacity plan for 10,000 engines with gold as a separate job.
- Documented every design decision (10 ADRs) and 16 real failures with root cause and fix; CI with pytest, including
  a Spark-vs-Python parity test for the scoring logic.

**Short version (one line):** Real-time jet-engine telemetry pipeline (Kafka → Spark Structured Streaming → Delta Lake)
on NASA data: exactly-once proven after a crash, 100% of engines alerted ~44 cycles before failure, load-tested.

## 30-second pitch

"I built a real-time pipeline for jet-engine sensor data using NASA's public run-to-failure dataset. A producer
replays 200 engines into Kafka; Spark Structured Streaming lands raw data in bronze, validates and deduplicates it in
silver with a dead-letter table for bad records, and keeps a health score per engine in gold that raises alerts. I
proved exactly-once by killing the Spark job mid-stream with chaos data and checking every event against the
producer's manifest: zero lost, zero duplicated. The most interesting part was finding that my alert model missed
44% of one fleet because a second fault type moved sensors in the opposite direction. I fixed it, validated on
held-out engines, and got detection to 100% with the same false-alarm rate."

## How it maps to the Cummins Data Engineer 1 posting

| Posting asks for | Where this project shows it |
|---|---|
| ETL/ELT pipelines, data ingestion | Kafka producer → bronze → silver → gold, replayable from bronze |
| Data quality, validation, trusted data | schema checks, dead-letter table with reasons, dedupe, exactly-once verification |
| Data modelling for analytics | gold: engine current state (one row per engine), alerts (one per engine × level), fleet health per minute |
| Spark / PySpark, big data | Structured Streaming, foreachBatch, applyInPandas, MERGE, watermarks, checkpoints |
| Kafka / streaming, IoT | partitions keyed by engine, idempotent producer, event time vs processing time |
| Performance and scale | load test, backpressure (`maxOffsetsPerTrigger`), capacity plan for 10,000 engines |
| AI/analytics readiness | calibrated health index, per-segment model evaluation, held-out validation |
| Documentation, working with others | architecture doc, ADRs, build log, project page a non-engineer can read |

## Be ready to answer (with where the answer lives)

1. Walk me through the architecture in 2 minutes. → `docs/architecture.md`, project page diagram
2. How do you guarantee exactly-once? How did you prove it? → build-log "Final demo run", `report.verify`
3. What happens to a malformed message? → ADR-006, silver dead-letter query
4. How do you handle late data? What does the watermark cost you? → ADR-005/008, build-log F11
5. Tell me about a hard bug. → F10 (lazy recomputation in foreachBatch) or F14 + Phase 11 (missed fault type)
6. How did you choose alert thresholds? How do you know they're good? → Phase 4 and 11, `scripts/calibrate.py`
7. How would you scale to 10,000 engines / 1 billion events a day? → `docs/capacity-plan.md`
8. What would you change for production / on Azure? → Event Hubs (Kafka API), Databricks jobs per layer, ADLS,
   Unity Catalog, schema registry, alerting on consumer lag and freshness
9. Why Spark, not Flink? Why Delta, not plain Parquet? → ADR-002
10. What did you NOT build? → no schema registry, single broker, no auth; say so and say how you'd add them

## Honest framing

Say "I built" for what you can explain without notes. Before an interview, do Part A of `MILESTONES.md` and re-run
`scripts/calibrate.py` and `scripts/loadtest.py` yourself so the numbers are ones you have seen on your own machine.
