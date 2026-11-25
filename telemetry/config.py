"""Central configuration. Override anything with environment variables."""
import os
from pathlib import Path

ROOT = Path(os.environ.get("TT_ROOT", Path(__file__).resolve().parents[1]))
DATA_DIR = ROOT / "data"
LAKE = Path(os.environ.get("TT_LAKE", ROOT / "lake"))

BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.environ.get("TT_TOPIC", "engine-telemetry")
PARTITIONS = int(os.environ.get("TT_PARTITIONS", "6"))

# Delta tables (bronze / silver / gold) and their streaming checkpoints
TABLES = {
    "bronze": LAKE / "bronze" / "telemetry_raw",
    "silver": LAKE / "silver" / "telemetry",
    "dlq": LAKE / "silver" / "dead_letter",
    "engine_state": LAKE / "gold" / "engine_current_state",
    "alerts": LAKE / "gold" / "engine_alerts",
    "fleet_minute": LAKE / "gold" / "fleet_health_minute",
}
CHECKPOINTS = LAKE / "_checkpoints"

# Streaming behaviour
WATERMARK = os.environ.get("TT_WATERMARK", "10 minutes")      # how late an event may arrive
TRIGGER = os.environ.get("TT_TRIGGER", "5 seconds")            # micro-batch interval
MAX_OFFSETS_PER_TRIGGER = int(os.environ.get("TT_MAX_OFFSETS", "200000"))  # backpressure cap
