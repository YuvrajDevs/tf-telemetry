# Capacity plan (fill in with YOUR measurements, Milestone T2)

## 1. Measured on my machine
| Setting | Value |
|---|---|
| Machine | (CPU cores / RAM) |
| Kafka partitions | |
| Trigger interval / maxOffsetsPerTrigger / shuffle partitions | |
| Producer peak (msgs/s) | ~28,000 (single process, measured during development) |
| Bronze peak processed rows/s | |
| Silver peak processed rows/s | |
| Latency event→silver p50 / p95 (steady state) | |
| Max sustainable rate before lag grows | |

## 2. Sizing for 10,000 engines (example method, replace with your numbers)
- A real engine reports every ~1 s in flight → assume 10,000 engines × 1 event/s = **10,000 events/s**, peak ×2.
- Event ≈ 900 bytes JSON (≈ 150 bytes as compressed Parquet) → 10,000 × 900 B = 9 MB/s ≈ **780 GB/day raw in Kafka**,
  ≈ 130 GB/day in Delta.
- Kafka: at ~10 MB/s comfortable per partition → 2 partitions by throughput, but **24 partitions** for consumer
  parallelism and growth; replication 3 → ~5.5 TB for 7-day retention.
- Spark: if one core sustains N rows/s in silver (your measurement), cores needed = 20,000 / N, + 50% headroom.
- Storage: compaction (`OPTIMIZE`) hourly; partition silver by event_date; 30 days hot, then cheaper tier.

## 3. What would change at 100×
Separate jobs per layer, autoscaling on consumer lag, schema registry (Avro), multi-AZ Kafka with RF=3,
state store tuning (RocksDB), Z-ordering by engine_id, alert routing to an incident system.
