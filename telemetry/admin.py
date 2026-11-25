"""Kafka topic admin: create / describe / delete the telemetry topic."""
from confluent_kafka.admin import AdminClient, NewTopic

from .config import BOOTSTRAP, PARTITIONS, TOPIC


def client():
    return AdminClient({"bootstrap.servers": BOOTSTRAP})


def create_topic(partitions: int = PARTITIONS, retention_hours: int = 168):
    admin = client()   # keep a reference until the futures resolve
    fut = admin.create_topics([NewTopic(TOPIC, num_partitions=partitions, replication_factor=1,
                                           config={"retention.ms": str(retention_hours * 3600 * 1000), "compression.type": "lz4"})])
    for name, f in fut.items():
        try:
            f.result()
            print(f"created topic {name} with {partitions} partitions, retention {retention_hours}h")
        except Exception as e:  # already exists, etc.
            print(f"{name}: {e}")


def delete_topic():
    admin = client()
    for name, f in admin.delete_topics([TOPIC]).items():
        try:
            f.result()
            print(f"deleted {name}")
        except Exception as e:
            print(f"{name}: {e}")


def describe():
    md = client().list_topics(timeout=10)
    t = md.topics.get(TOPIC)
    if not t:
        print(f"{TOPIC} does not exist")
        return
    print(f"{TOPIC}: {len(t.partitions)} partitions")
    for p in sorted(t.partitions.values(), key=lambda x: x.id):
        print(f"  partition {p.id} leader={p.leader} replicas={p.replicas}")
