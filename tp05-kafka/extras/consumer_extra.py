"""Extended consumer for the "Further Investigation" tasks.

Features:
  * running average temperature (over the events that pass the filter,
    i.e. over all received events when no filter is set)
  * --group-id : choose the consumer group (compare two groups / same group)
  * --building : only show events from one campus building
  * --csv      : save consumed events to a CSV file
                 (columns: event_id, sensor_id, temperature_c, humidity_pct, timestamp)
  * --idle-timeout : stop automatically after N seconds without new messages

Examples:
    python extras/consumer_extra.py
    python extras/consumer_extra.py --group-id tp05-second-group
    python extras/consumer_extra.py --building building-A
    python extras/consumer_extra.py --csv events.csv --idle-timeout 10
"""

import argparse
import csv
import json

from kafka import KafkaConsumer

CSV_COLUMNS = ["event_id", "sensor_id", "temperature_c", "humidity_pct", "timestamp"]
ALERT_THRESHOLD_C = 35


def parse_args():
    parser = argparse.ArgumentParser(description="Extended campus-sensors consumer")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="campus-sensors")
    parser.add_argument("--group-id", default="tp05-extra-monitor")
    parser.add_argument("--building", help="only keep events of this campus_building")
    parser.add_argument("--csv", help="path of a CSV file to write")
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        help="stop after this many seconds without messages (default: run until Ctrl+C)",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id=args.group_id,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        consumer_timeout_ms=int(args.idle_timeout * 1000) if args.idle_timeout else float("inf"),
    )

    csv_file = None
    writer = None
    if args.csv:
        csv_file = open(args.csv, "w", newline="", encoding="utf-8")
        writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()

    print(
        f"Listening to topic: {args.topic} | group_id={args.group_id}"
        + (f" | building={args.building}" if args.building else "")
    )

    count = 0
    total_temperature = 0.0

    try:
        for message in consumer:
            event = message.value

            if args.building and event.get("campus_building") != args.building:
                continue

            temperature = event["temperature_c"]
            count += 1
            total_temperature += temperature
            average = total_temperature / count

            print(
                f"[p{message.partition}@{message.offset}] "
                f'{event["timestamp"]} | {event["sensor_id"]} | '
                f'{event.get("campus_building", "-")} | '
                f'T={temperature}°C | RH={event["humidity_pct"]}% | '
                f"running avg T={average:.2f}°C (n={count})"
            )
            if temperature >= ALERT_THRESHOLD_C:
                print("ALERT: High temperature detected!")

            if writer:
                writer.writerow(event)
                csv_file.flush()
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        if count:
            print(f"Summary: {count} events, average temperature = {total_temperature / count:.2f}°C")
        if csv_file:
            csv_file.close()
        consumer.close()


if __name__ == "__main__":
    main()
