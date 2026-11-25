"""Bronze: Kafka -> Delta, exactly as received (plus Kafka coordinates and ingest time).

Nothing is parsed or dropped here. Bronze is the replayable record of what arrived.
"""
from pyspark.sql import functions as F

from ..config import BOOTSTRAP, CHECKPOINTS, MAX_OFFSETS_PER_TRIGGER, TABLES, TOPIC, TRIGGER


def start(spark):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", MAX_OFFSETS_PER_TRIGGER)   # backpressure: cap each micro-batch
           .load())
    out = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingest_ts"),
    ).withColumn("ingest_date", F.to_date("ingest_ts"))
    return (out.writeStream.format("delta").queryName("bronze")
            .outputMode("append")
            .option("checkpointLocation", str(CHECKPOINTS / "bronze"))   # offsets + commit log = exactly-once into Delta
            .partitionBy("ingest_date")
            .trigger(processingTime=TRIGGER)
            .start(str(TABLES["bronze"])))
