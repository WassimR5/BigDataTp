"""TP06 producer: sends synthetic check-in events to the Kafka topic campus-events.

Default behaviour = the lab sheet (7 events, one per second, one of them out of order):
    python producer.py

Optional arguments (for Task 5 and the further-investigation tasks):
    --campus-time HH:MM   events start at this campus local time (UTC+1, Eloued)
                          e.g. --campus-time 08:20 produces check-ins AFTER 08:15
    --extra               also send 'library-entry' events (counts by event type)
    --count N             send N random events instead of the 7 fixed ones
    --interval S          seconds between two events (default 1)
"""

import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer

TOPIC = "campus-events"
DEPARTMENTS = ["Computer Science", "Mathematics", "Physics"]
CAMPUS_UTC_OFFSET = 1  # Algeria (Eloued) = UTC+1


def parse_args():
    parser = argparse.ArgumentParser(description="TP06 campus events producer")
    parser.add_argument("--campus-time", help="start time on campus, format HH:MM")
    parser.add_argument("--extra", action="store_true", help="add library-entry events")
    parser.add_argument("--count", type=int, help="number of random events")
    parser.add_argument("--interval", type=float, default=1.0)
    return parser.parse_args()


def start_time(campus_time):
    """Base time in UTC. Without --campus-time, use the current time."""
    if not campus_time:
        return datetime.now(timezone.utc).replace(microsecond=0)
    hour, minute = map(int, campus_time.split(":"))
    local_today = datetime.now(timezone.utc) + timedelta(hours=CAMPUS_UTC_OFFSET)
    local_start = local_today.replace(hour=hour, minute=minute, second=0, microsecond=0)
    return (local_start - timedelta(hours=CAMPUS_UTC_OFFSET)).replace(tzinfo=timezone.utc)


def build_events(args):
    if args.count:
        types = ["check-in", "check-in", "check-in", "open-course"]
        if args.extra:
            types.append("library-entry")
        return [
            (
                f"S{random.randint(100, 599)}",
                random.choice(types),
                random.choice(DEPARTMENTS),
                random.randint(0, 120),
            )
            for _ in range(args.count)
        ]

    events = [
        ("S101", "check-in", "Computer Science", 2),
        ("S204", "check-in", "Mathematics", 8),
        ("S101", "open-course", "Computer Science", 15),
        ("S305", "check-in", "Physics", 22),
        ("S411", "check-in", "Computer Science", 35),
        ("S204", "check-in", "Mathematics", 48),
        ("S512", "check-in", "Physics", 12),  # deliberately out of order
    ]
    if args.extra:
        events += [
            ("S101", "library-entry", "Computer Science", 40),
            ("S305", "library-entry", "Physics", 44),
        ]
    return events


def main():
    args = parse_args()
    base = start_time(args.campus_time)

    producer = KafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
    )

    try:
        for event_id, (student, event_type, dept, seconds) in enumerate(build_events(args), 1):
            event_time = base + timedelta(seconds=seconds)
            event = {
                "eventId": event_id,
                "studentId": student,
                "eventType": event_type,
                "department": dept,
                "eventTime": event_time.isoformat(),
            }
            producer.send(TOPIC, value=event)
            print("Sent:", event)
            time.sleep(args.interval)
        producer.flush()
    finally:
        producer.close()


if __name__ == "__main__":
    main()
