"""The silver job computes the health index in Spark; gold and the unit tests use the Python version.
They must agree exactly, or alerts would differ from what was calibrated. Skipped if PySpark is not installed."""
import pytest

pyspark = pytest.importorskip("pyspark")

from telemetry import cmapss  # noqa: E402
from telemetry.events import FAN_SIGNS, HEALTH_SENSORS, HPC_SIGNS, fault_mode, health_index  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession
    s = SparkSession.builder.master("local[1]").appName("parity").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_spark_and_python_health_index_agree(spark):
    from telemetry.jobs.silver import fault_mode_expr, health_expr
    rows = cmapss.synthetic("FD001", engines=2)[:50]
    for signs, z in ((FAN_SIGNS, 8.0), (HPC_SIGNS, 4.0)):
        r = dict(rows[0])
        for s, (_, mu, sd) in HEALTH_SENSORS.items():
            r[s] = mu + signs[s] * z * sd
        rows.append(r)
    df = spark.createDataFrame([{k: float(v) for k, v in r.items() if k.startswith("s")} for r in rows])
    got = df.select(health_expr().alias("hi"), fault_mode_expr().alias("fm")).collect()
    for r, g in zip(rows, got):
        assert g.hi == pytest.approx(health_index(r), abs=1e-9)
        assert g.fm == fault_mode(r)
