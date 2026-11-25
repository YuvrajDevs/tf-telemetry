# Capacity plan (Milestone T2)

Measured with `scripts/loadtest.py`. Raw results: [`loadtest-results.json`](loadtest-results.json) (all layers) and
[`loadtest-results-no-gold.json`](loadtest-results-no-gold.json) (bronze + silver only). Re-run it on your own machine
and replace these numbers with yours.

## 1. Test setup
| Setting | Value |
|---|---|
| Machine | Linux container, 4 CPU cores, 16 GB RAM (laptop-class) |
| Kafka | 1 broker (KRaft, Docker), topic with 6 partitions |
| Spark | `local[*]`, one application running 5 streaming queries (bronze, silver, dead letter, engine state, fleet minute) |
| Trigger / backpressure | 5 s micro-batches, `maxOffsetsPerTrigger` = 200,000 |
| Events | real NASA C-MAPSS rows as JSON (~900 bytes each) |

Two kinds of test:
1. **Backlog drain:** put ~450,000 events in Kafka first, then start the pipeline cold and time how long until all of
   them are in silver. This shows the maximum processing rate.
2. **Steady state:** with the pipeline running, produce at a fixed rate for 60 s and measure end-to-end latency
   (`event_time` set by the producer → row committed in silver) and whether the pipeline keeps up.

## 2. Results
| Measurement | Result |
|---|---|
| Producer, single Python process, no throttle | **~28,000-30,000 events/s** |
| Backlog of 453,510 events, cold start (JVM + connectors ~35-50 s included) | all in silver after **59 s** |
| Bronze batch during catch-up | ~200,000 rows committed every ~6.5 s → **~30,000 events/s** |
| Gold (stateful `applyInPandas` + `MERGE`) peak | **~14,000 events/s**, batches take **~30 s** (median) |
| Steady 1,000 events/s, all layers | keeps up; latency p50 **14.0 s**, p95 **23.7 s**, p99 26.1 s |
| Steady 3,000 events/s, all layers | keeps up; latency p50 **17.8 s**, p95 **24.6 s**, p99 27.0 s |
| Steady 3,000 events/s, **bronze + silver only** | keeps up; latency p50 **8.2 s**, p95 **10.8 s**, p99 11.6 s |

**Delta commit history tells you what each layer is doing** (`DeltaTable(path).history()`):

| Layer | Median time between commits, all layers running | Without gold |
|---|---|---|
| bronze | 6.6 s | 5.0 s (on schedule) |
| silver | 6.9 s | 4.4 s |
| gold engine state | 30.1 s | — |

## 3. What the numbers say
- **The bottleneck is the stateful gold step, not Kafka or bronze.** Each gold batch runs `applyInPandas` over every
  engine in the batch (Python workers, Arrow transfer), then two `MERGE`s. That takes ~30 s and uses the same 4 cores
  as everything else, so bronze and silver fall behind their 5 s trigger ("Current batch is falling behind" warnings, F7).
- **Latency is mostly micro-batch waiting.** An event waits for the bronze batch (up to 5 s + run time), then for the
  silver batch (the same again). Removing CPU contention (no gold) halves latency: p95 24.6 s → 10.8 s.
- **The pipeline keeps up at 3,000 events/s** on 4 cores with all layers: silver was complete 27 s after the producer
  stopped, roughly one gold cycle.

## 4. Changes, in order of value
1. **Run gold as its own job** (its own cores / cluster) with a longer trigger (e.g. 30 s). Gold doesn't need
   second-level freshness: one reading per engine per flight cycle. Bronze and silver keep their 5 s cadence.
2. **Shorter trigger for bronze/silver** (`TT_TRIGGER="1 second"`) if lower latency is needed: about 2 × trigger + run time.
3. **More partitions** (12-24) so more cores can read Kafka in parallel, and `spark.sql.shuffle.partitions` matched to cores.
4. **Compaction** (`OPTIMIZE`) on silver: 5 s batches create many small files (Milestone T6).

## 5. Sizing for 10,000 engines
**Load:** a real engine reports about once per second in flight → 10,000 events/s, plan for **20,000/s at peak**.
Event ≈ 900 B as JSON → ~9 MB/s, **~780 GB/day raw in Kafka**, ~130 GB/day in Delta (Parquet compresses ~6×).

| Component | Estimate (linear from the measurements above, +50% headroom) |
|---|---|
| Kafka | ~18 MB/s at peak: throughput needs only a few partitions, but use **24 partitions** for consumer parallelism and growth; 3 brokers, replication 3 → ~16 TB for 7 days of retention |
| Bronze + silver job | measured ~30,000 events/s on 4 shared cores ≈ 7,500/s per core → 20,000/s ≈ 3 cores, **6-8 cores** with headroom and room for silver parsing |
| Gold job | measured ~14,000/s on 4 shared cores ≈ 3,500/s per core → 20,000/s ≈ 6 cores, **~9-10 cores**, trigger 30 s |
| Storage | silver partitioned by `event_date`; hourly `OPTIMIZE`; 30 days hot, then a cheaper storage tier |

These are first estimates from one small machine; at this scale you would re-run the same load test on the target
cluster before committing to a size.

## 6. What would change at 100× (1 million engines)
Separate clusters per layer with autoscaling on consumer lag, a schema registry (Avro/Protobuf instead of JSON),
the RocksDB state store or a key-value store for engine state instead of a full `MERGE` per batch, Z-ordering
silver by `engine_id`, and alert routing to an incident system. On Azure: Event Hubs (Kafka API) + Databricks jobs +
ADLS Gen2, with the same code.
