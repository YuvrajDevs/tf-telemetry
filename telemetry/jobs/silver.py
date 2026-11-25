"""Silver: parse, validate, deduplicate. Bad records go to a dead-letter table, never silently dropped.

Two streaming queries read the same bronze table:
  silver      valid events, typed, health index added, de-duplicated on event_id within the watermark
  dead_letter invalid events with the raw payload and a reason code
"""
from functools import reduce

from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..cmapss import SENSORS
from ..config import CHECKPOINTS, TABLES, TRIGGER, WATERMARK
from ..events import HEALTH_SENSORS, SENSOR_RANGES

EVENT_SCHEMA = T.StructType([
    T.StructField("schema_version", T.IntegerType()),
    T.StructField("event_id", T.StringType()),
    T.StructField("fleet", T.StringType()),
    T.StructField("engine_id", T.StringType()),
    T.StructField("cycle", T.IntegerType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("op_settings", T.ArrayType(T.DoubleType())),
    T.StructField("sensors", T.StructType([T.StructField(s, T.DoubleType()) for s in SENSORS])),
])


def classify(bronze_df):
    """Adds `e` (parsed struct), `event_ts` and `dlq_reason` (NULL = valid). Same rules as events.validate()."""
    e = F.col("e")
    df = (bronze_df.withColumn("e", F.from_json("raw_value", EVENT_SCHEMA))
          .withColumn("event_ts", F.to_timestamp(F.col("e.event_time"))))
    malformed = e.isNull() | (e["event_id"].isNull() & e["engine_id"].isNull() & e["cycle"].isNull() & e["sensors"].isNull())
    missing = reduce(lambda a, b: a | b, [e[f].isNull() for f in ("event_id", "engine_id", "cycle", "event_time", "sensors")]
                     + [F.col("event_ts").isNull()] + [e["sensors"][s].isNull() for s in SENSORS])
    out_of_range = reduce(lambda a, b: a | b, [(e["sensors"][s] < lo) | (e["sensors"][s] > hi) for s, (lo, hi) in SENSOR_RANGES.items()])
    reason = (F.when(malformed, "MALFORMED_JSON").when(missing, "MISSING_FIELD").when(out_of_range, "OUT_OF_RANGE"))
    return df.withColumn("dlq_reason", reason)


def health_expr():
    terms = [F.lit(float(sign)) * (F.col(s) - F.lit(mu)) / F.lit(sd) for s, (sign, mu, sd) in HEALTH_SENSORS.items()]
    return reduce(lambda a, b: a + b, terms) / F.lit(float(len(terms)))


def valid_events(classified):
    v = classified.filter(F.col("dlq_reason").isNull())
    return (v.select(
                F.col("e.event_id").alias("event_id"), F.col("e.fleet").alias("fleet"),
                F.col("e.engine_id").alias("engine_id"), F.col("e.cycle").alias("cycle"),
                F.col("event_ts").alias("event_time"),
                *[F.col("e.sensors")[s].alias(s) for s in SENSORS],
                F.col("e.op_settings").alias("op_settings"),
                "kafka_ts", "ingest_ts", "partition", "offset")
            .withColumn("health_index", health_expr())
            .withColumn("event_date", F.to_date("event_time")))


def start(spark):
    bronze = spark.readStream.format("delta").load(str(TABLES["bronze"]))
    classified = classify(bronze)

    silver = (valid_events(classified)
              .withWatermark("event_time", WATERMARK)
              .dropDuplicatesWithinWatermark(["event_id"])          # at-least-once in, exactly-once out
              .withColumn("silver_ts", F.current_timestamp()))
    q_silver = (silver.writeStream.format("delta").queryName("silver")
                .outputMode("append")
                .option("checkpointLocation", str(CHECKPOINTS / "silver"))
                .partitionBy("event_date")
                .trigger(processingTime=TRIGGER)
                .start(str(TABLES["silver"])))

    dlq = (classified.filter(F.col("dlq_reason").isNotNull())
           .select("raw_value", "dlq_reason", "kafka_key", "partition", "offset", "kafka_ts", "ingest_ts",
                   F.current_timestamp().alias("dlq_ts")))
    q_dlq = (dlq.writeStream.format("delta").queryName("dead_letter")
             .outputMode("append")
             .option("checkpointLocation", str(CHECKPOINTS / "dead_letter"))
             .trigger(processingTime=TRIGGER)
             .start(str(TABLES["dlq"])))
    return [q_silver, q_dlq]
