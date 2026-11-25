# Architecture

```mermaid
flowchart LR
    subgraph Source["Source (simulated fleet)"]
        D[NASA C-MAPSS<br/>200 engines run-to-failure] --> P[Producer<br/>key = engine_id<br/>acks=all, idempotent<br/>chaos: dup / late / bad]
    end
    P -->|JSON events| K[(Kafka topic<br/>engine-telemetry<br/>6 partitions, 7 d retention)]
    subgraph Spark["Spark Structured Streaming (micro-batches every 5 s)"]
        B[Bronze query<br/>raw + Kafka coordinates<br/>maxOffsetsPerTrigger]
        S[Silver query<br/>parse · validate · health index<br/>dedupe within 10 min watermark]
        Q[Dead-letter query<br/>raw payload + reason]
        G[Engine-state query<br/>foreachBatch: applyInPandas + MERGE<br/>EWMA, escalating alerts]
        F[Fleet query<br/>1-min event-time windows<br/>append, 2 min watermark]
    end
    K --> B --> BT[(bronze.telemetry_raw)]
    BT --> S --> ST[(silver.telemetry)]
    BT --> Q --> QT[(silver.dead_letter)]
    ST --> G --> GS[(gold.engine_current_state)]
    G --> GA[(gold.engine_alerts)]
    ST --> F --> GF[(gold.fleet_health_minute)]
    GS & GA & GF & QT --> UI[Streamlit dashboard]
    ST --> V{{verify: manifest vs silver<br/>exactly-once proof}}
```

## Guarantees and how each is achieved

| Guarantee | Mechanism |
|---|---|
| No message lost between Kafka and bronze | Spark checkpoint stores offsets; a batch's offsets are committed only after the Delta write commits |
| No partial files | Delta Lake atomic commits (transaction log) |
| Duplicates removed | Deterministic `event_id` + `dropDuplicatesWithinWatermark` (10 min) |
| Bad data never blocks good data | Validation in silver; invalid rows to the dead-letter table with a reason |
| Per-engine ordering | Kafka key = engine_id (same partition); state applies events in cycle order |
| Alerts raised exactly once | MERGE on `alert_id = engine:LEVEL`, insert-only |
| Re-running a batch is harmless | State ignores cycles ≤ last applied; alert ids already exist |
| Bounded memory | Watermarks drop old dedupe/window state; RocksDB state store |
| Controlled catch-up after outage | `maxOffsetsPerTrigger` |

## Tables

| Table | Grain | Key columns |
|---|---|---|
| bronze.telemetry_raw | one Kafka message | partition, offset |
| silver.telemetry | one valid engine cycle | event_id |
| silver.dead_letter | one rejected message | partition, offset, dlq_reason |
| gold.engine_current_state | one engine | engine_id |
| gold.engine_alerts | one engine × level | alert_id |
| gold.fleet_health_minute | one fleet × minute | window_start, fleet |
