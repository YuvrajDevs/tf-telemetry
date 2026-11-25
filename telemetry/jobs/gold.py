"""Gold: engine current state + idempotent alerts (stateful, via foreachBatch + MERGE), and fleet health per minute.

engine_current_state  one row per engine: last cycle, smoothed health (EWMA), alert level
engine_alerts         one row per (engine, level) the first time the level is reached
fleet_health_minute   1-minute event-time windows per fleet (append mode, closed by the watermark)
"""
import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql import types as T

from ..config import CHECKPOINTS, TABLES, TRIGGER
from ..events import advance_state

STATE_SCHEMA = T.StructType([
    T.StructField("engine_id", T.StringType()), T.StructField("fleet", T.StringType()),
    T.StructField("last_cycle", T.IntegerType()), T.StructField("last_event_time", T.TimestampType()),
    T.StructField("health_index", T.DoubleType()), T.StructField("ewma_health", T.DoubleType()),
    T.StructField("alert_level", T.IntegerType()), T.StructField("s11", T.DoubleType()), T.StructField("s4", T.DoubleType()),
])
ALERT_SCHEMA = T.StructType([
    T.StructField("alert_id", T.StringType()), T.StructField("engine_id", T.StringType()), T.StructField("fleet", T.StringType()),
    T.StructField("level", T.StringType()), T.StructField("cycle", T.IntegerType()), T.StructField("ewma_health", T.DoubleType()),
    T.StructField("event_time", T.TimestampType()),
])
# applyInPandas returns one schema, so state rows and alert rows share it with a `kind` column
OUT_SCHEMA = T.StructType([T.StructField("kind", T.StringType())] + STATE_SCHEMA.fields
                          + [T.StructField("alert_id", T.StringType()), T.StructField("level", T.StringType())])
PREV = ["last_cycle", "ewma_health", "alert_level", "fleet", "last_event_time", "health_index", "s11", "s4"]


def ensure_tables(spark):
    for name, schema in (("engine_state", STATE_SCHEMA), ("alerts", ALERT_SCHEMA)):
        if not DeltaTable.isDeltaTable(spark, str(TABLES[name])):
            spark.createDataFrame([], schema).write.format("delta").save(str(TABLES[name]))


def _fold(pdf: pd.DataFrame) -> pd.DataFrame:
    """All events of ONE engine in this micro-batch (+ its previous state) -> new state row + alert rows."""
    first = pdf.iloc[0]
    prev = None
    if pd.notna(first["prev_last_cycle"]):
        prev = {"engine_id": first["engine_id"], **{c: first[f"prev_{c}"] for c in PREV}}
        prev["last_cycle"], prev["alert_level"] = int(prev["last_cycle"]), int(prev["alert_level"])
    events = pdf[["engine_id", "fleet", "cycle", "event_time", "health_index", "s11", "s4"]].to_dict("records")
    state, alerts = advance_state(prev, events)
    rows = []
    if state and (prev is None or state["last_cycle"] != prev["last_cycle"]):
        rows.append({"kind": "state", **state})
    for a in alerts:
        rows.append({"kind": "alert", "engine_id": a["engine_id"], "fleet": a["fleet"], "last_cycle": a["cycle"],
                     "ewma_health": a["ewma_health"], "last_event_time": a["event_time"], "alert_id": a["alert_id"], "level": a["level"]})
    cols = [f.name for f in OUT_SCHEMA.fields]
    return pd.DataFrame(rows, columns=cols)


def upsert_state_and_alerts(batch, batch_id):
    spark = batch.sparkSession
    events = batch.select("engine_id", "fleet", "cycle", "event_time", "health_index", "s11", "s4")
    prev = spark.read.format("delta").load(str(TABLES["engine_state"])).select(
        "engine_id", *[F.col(c).alias(f"prev_{c}") for c in PREV])
    # IMPORTANT: materialise the result ONCE before writing anything. `out` is computed from the
    # current engine_state table; if Spark lazily recomputed it after we MERGE new state, every
    # event would look "already applied" and alerts would silently vanish (bug found in testing,
    # see docs/build-log.md). localCheckpoint(eager=True) computes it now and cuts the lineage.
    out = (events.join(prev, "engine_id", "left")
           .groupBy("engine_id").applyInPandas(_fold, OUT_SCHEMA)
           .localCheckpoint(eager=True))
    alerts = out.filter("kind = 'alert'").select(
        "alert_id", "engine_id", "fleet", "level", F.col("last_cycle").alias("cycle"), "ewma_health",
        F.col("last_event_time").alias("event_time"))
    (DeltaTable.forPath(spark, str(TABLES["alerts"])).alias("t")
     .merge(alerts.alias("s"), "t.alert_id = s.alert_id")
     .whenNotMatchedInsertAll()                                            # replaying a batch can't raise an alert twice
     .execute())
    state = out.filter("kind = 'state'").select(*[f.name for f in STATE_SCHEMA.fields])
    (DeltaTable.forPath(spark, str(TABLES["engine_state"])).alias("t")
     .merge(state.alias("s"), "t.engine_id = s.engine_id")
     .whenMatchedUpdateAll(condition="s.last_cycle > t.last_cycle")       # never move an engine backwards
     .whenNotMatchedInsertAll()
     .execute())


def start(spark):
    ensure_tables(spark)
    silver = spark.readStream.format("delta").load(str(TABLES["silver"]))
    q_state = (silver.writeStream.queryName("engine_state")
               .foreachBatch(upsert_state_and_alerts)
               .option("checkpointLocation", str(CHECKPOINTS / "engine_state"))
               .trigger(processingTime=TRIGGER)
               .start())
    fleet = (silver.withWatermark("event_time", "2 minutes")
             .groupBy(F.window("event_time", "1 minute").alias("w"), "fleet")
             .agg(F.count("*").alias("events"),
                  F.approx_count_distinct("engine_id").alias("engines"),
                  F.avg("health_index").alias("avg_health_index"),
                  F.max("health_index").alias("max_health_index"))
             .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "fleet",
                     "events", "engines", "avg_health_index", "max_health_index"))
    q_fleet = (fleet.writeStream.format("delta").queryName("fleet_health_minute")
               .outputMode("append")
               .option("checkpointLocation", str(CHECKPOINTS / "fleet_health_minute"))
               .trigger(processingTime=TRIGGER)
               .start(str(TABLES["fleet_minute"])))
    return [q_state, q_fleet]
