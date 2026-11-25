"""SparkSession with Delta Lake and the Kafka connector (jars are downloaded from Maven on first run)."""
import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"


def get_spark(app: str = "turbofan-telemetry") -> SparkSession:
    # Python workers must use this same interpreter/venv (pandas + pyarrow for applyInPandas)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (SparkSession.builder.appName(app)
               .master(os.environ.get("SPARK_MASTER", "local[*]"))
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.shuffle.partitions", os.environ.get("TT_SHUFFLE_PARTITIONS", "8"))
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.streaming.stateStore.providerClass",
                       "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
               .config("spark.driver.memory", os.environ.get("TT_DRIVER_MEMORY", "3g"))
               .config("spark.ui.showConsoleProgress", "false"))
    spark = configure_spark_with_delta_pip(builder, extra_packages=[KAFKA_PKG]).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
