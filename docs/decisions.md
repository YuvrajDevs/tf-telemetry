# Architecture decision records (short form)

**ADR-001 Kafka (KRaft) as the event log.** Industry standard, replayable, partitioned for parallelism and per-key ordering.
Alternatives: Event Hubs/Kinesis (managed, cloud-only), RabbitMQ (queue, not a replayable log).

**ADR-002 Spark Structured Streaming + Delta Lake.** Same engine and API as batch PySpark; checkpoints + Delta give
exactly-once into tables; MERGE enables stateful upserts. Alternative: Flink (lower latency, separate skill set).

**ADR-003 Medallion layers.** Bronze stores exactly what arrived (replayable), silver is clean and deduplicated,
gold is business-ready. A bug in silver/gold is fixed by reprocessing from bronze.

**ADR-004 Health index v2 = max(HPC pattern, 0.4 × fan pattern) of 10 drifting sensors (signed z-scores), EWMA α=0.2,
WARNING ≥ 1.5, CRITICAL ≥ 2.5.** v1 used only the HPC pattern and missed 44% of FD003 (fan fault, build-log F14).
Calibrated with `scripts/calibrate.py` on all 200 NASA run-to-failure engines: 100% of FD001 and FD003 warned before
failure, warning median 44 cycles ahead (p10 25), critical median 17-18 (p10 8-11), 1-2 warnings per 100 engines
earlier than 125 cycles. Fan weight tuned on odd FD003 engines, validated on even ones. Alternative rejected: mean
absolute deviation (later warnings on FD001, 16 false alarms on FD003). Next step if more fault types appear: one
pattern per fault, or a learned model, with the same per-segment evaluation.

**ADR-005 Dedupe within a 10-minute watermark, not globally.** Bounded state and cost; covers redelivery after
crashes and retries. Duplicates arriving > 10 min later are not caught (documented trade-off, Milestone T4).

**ADR-006 Dead-letter table instead of failing the stream.** One malformed message must never stop millions of good ones.

**ADR-007 Stateful gold via foreachBatch + applyInPandas + MERGE (not applyInPandasWithState).** The state lives in a
queryable Delta table (dashboards read it directly), logic is a pure Python function shared with unit tests, and
replays are idempotent. Requires materialising the batch before writing (build-log F10).

**ADR-008 Event time, not processing time.** Late events land in the right window; watermarks bound how late.

**ADR-009 Everything local and free.** Reviewers can run it with Docker + Python; the same code runs on Databricks by
changing the master and storage paths.

**ADR-010 Label each alert with its fault mode (HPC / FAN).** It costs nothing (the winning pattern is already computed)
and lets alerts be routed to the right team. Added as a nullable column, so old rows stay valid.
