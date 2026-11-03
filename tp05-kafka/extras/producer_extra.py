"""Extended producer for the "Further Investigation" tasks.

Differences from producer.py:
  * the number of events is configurable (--count, default 100)
  * every event has an extra field "campus_building"
  * optional --key-by-sensor: use sensor_id as the Kafka message key, so all
    events of one sensor go to the same partition
  * prints the partition and offset assigned by the broker to each event

Examples:
    python extras/producer_extra.py
    python extras/producer_extra.py --count 100 --interval 0.2
    python extras/producer_extra.py --count 30 --key-by-sensor
"""

import argparse
import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

BUILDINGS = ["building-A", "building-B", "building-C"]


def parse_args():
    parser = argparse.ArgumentParser(description="Extended campus-sensors producer")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="campus-sensors")
    parser.add_argument("--count", type=int, default=100, help="number of events")
    parser.add_argument(
        "--interval", type=float, default=1.0, help="seconds between two events"
    )
    parser.add_argument(
        "--key-by-sensor",
        action="store_true",
        help="use sensor_id as message key (same sensor -> same partition)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
    )

    try:
        for event_id in range(1, args.count + 1):
            sensor_id = f"sensor-{random.randint(1, 4):02d}"
            event = {
                "event_id": event_id,
                "sensor_id": sensor_id,
                "campus_building": random.choice(BUILDINGS),
                "temperature_c": round(random.uniform(18, 42), 2),
                "humidity_pct": round(random.uniform(25, 85), 2),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            key = sensor_id if args.key_by_sensor else None
            metadata = producer.send(args.topic, key=key, value=event).get(timeout=10)
            print(f"Sent (partition={metadata.partition}, offset={metadata.offset}):", event)
            time.sleep(args.interval)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
