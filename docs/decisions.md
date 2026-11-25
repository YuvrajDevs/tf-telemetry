# Architecture decision records (short form)

**ADR-001 Kafka (KRaft) as the event log.** Industry standard, replayable, partitioned for parallelism and per-key ordering.
Alternatives: Event Hubs/Kinesis (managed, cloud-only), RabbitMQ (queue, not a replayable log).

**ADR-002 Spark Structured Streaming + Delta Lake.** Same engine and API as batch PySpark; checkpoints + Delta give
exactly-once into tables; MERGE enables stateful upserts. Alternative: Flink (lower latency, separate skill set).

**ADR-003 Medallion layers.** Bronze stores exactly what arrived (replayable), silver is clean and deduplicated,
gold is business-ready. A bug in silver/gold is fixed by reprocessing from bronze.

**ADR-004 Health index = mean signed z-score of 10 drifting sensors, EWMA α=0.2, WARNING ≥ 1.5, CRITICAL ≥ 2.5.**
Calibrated on FD001 run-to-failure data: warning median 44 cycles before failure (p10 27), critical median 19 (p10 12),
100% of FD001 engines caught. Known limitation: FD003 fan-fault engines are missed (build-log F14, Milestone T1).

**ADR-005 Dedupe within a 10-minute watermark, not globally.** Bounded state and cost; covers redelivery after
crashes and retries. Duplicates arriving > 10 min later are not caught (documented trade-off, Milestone T4).

**ADR-006 Dead-letter table instead of failing the stream.** One malformed message must never stop millions of good ones.

**ADR-007 Stateful gold via foreachBatch + applyInPandas + MERGE (not applyInPandasWithState).** The state lives in a
queryable Delta table (dashboards read it directly), logic is a pure Python function shared with unit tests, and
replays are idempotent. Requires materialising the batch before writing (build-log F10).

**ADR-008 Event time, not processing time.** Late events land in the right window; watermarks bound how late.

**ADR-009 Everything local and free.** Reviewers can run it with Docker + Python; the same code runs on Databricks by
changing the master and storage paths.
