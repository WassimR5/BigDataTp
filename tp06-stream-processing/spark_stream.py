"""TP06 - Spark Structured Streaming on the Kafka topic campus-events.

Default (Task 3): check-ins per department in 1-minute event-time windows.

    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream.py

Options (one change at a time for the Task 4 experiments):
    --trigger 5            Experiment A: processing-time trigger of 5 seconds
    --window "2 minutes"   Experiment B: window duration
    --slide "1 minute"     Experiment C: sliding window (with --window "2 minutes")
    --watermark "2 minutes" Experiment D: watermark delay
    --mode counts|late|by-type|both
                           counts  : check-ins per department per window (default)
                           late    : Task 5, check-ins after 08:15 campus time
                           by-type : counts by event type and department
                           both    : counts + late as two independent queries
    --checkpoint DIR       base checkpoint folder (default ./checkpoint)
                           Keep it unchanged for the recovery experiment (E).
"""

import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, from_json, from_utc_timestamp, to_timestamp, window
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

CAMPUS_TZ = "Africa/Algiers"  # UTC+1: used only for the 08:15 rule
LATE_AFTER = "08:15:00"


def parse_args():
    parser = argparse.ArgumentParser(description="TP06 Spark Structured Streaming")
    parser.add_argument("--mode", default="counts", choices=["counts", "late", "by-type", "both"])
    parser.add_argument("--window", default="1 minute")
    parser.add_argument("--slide", default=None, help="slide duration (sliding window)")
    parser.add_argument("--watermark", default="30 seconds")
    parser.add_argument("--trigger", type=int, default=None, help="processing-time trigger in seconds")
    parser.add_argument("--checkpoint", default="./checkpoint")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--topic", default="campus-events")
    parser.add_argument("--starting-offsets", default="earliest")
    return parser.parse_args()


def start_query(df, output_mode, name, args):
    """Write a streaming DataFrame to the console. Each query has its own checkpoint folder."""
    writer = (
        df.writeStream.outputMode(output_mode)
        .format("console")
        .option("truncate", "false")
        .option("checkpointLocation", f"{args.checkpoint}/{name}")
        .queryName(name)
    )
    if args.trigger:
        writer = writer.trigger(processingTime=f"{args.trigger} seconds")
    return writer.start()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("CampusRealTimeAnalytics").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # event times are UTC

    # The schema is defined explicitly: Kafka only gives bytes, Spark must know the fields/types.
    schema = StructType(
        [
            StructField("eventId", IntegerType(), True),
            StructField("studentId", StringType(), True),
            StructField("eventType", StringType(), True),
            StructField("department", StringType(), True),
            StructField("eventTime", StringType(), True),
        ]
    )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )

    json_events = raw.selectExpr("CAST(value AS STRING) AS json")
    parsed = (
        json_events.select(from_json(col("json"), schema).alias("e"))
        .select("e.*")
        .withColumn("event_ts", to_timestamp(col("eventTime")))
        .filter(col("event_ts").isNotNull())
    )

    if args.slide:
        win = window(col("event_ts"), args.window, args.slide)
    else:
        win = window(col("event_ts"), args.window)

    queries = []

    if args.mode in ("counts", "both"):
        checkins = parsed.filter(col("eventType") == "check-in")
        counts = checkins.withWatermark("event_ts", args.watermark).groupBy(win, col("department")).count()
        queries.append(start_query(counts, "update", "counts", args))

    if args.mode == "by-type":
        by_type = (
            parsed.withWatermark("event_ts", args.watermark)
            .groupBy(win, col("eventType"), col("department"))
            .count()
        )
        queries.append(start_query(by_type, "update", "by-type", args))

    if args.mode in ("late", "both"):
        # Task 5: late check-in = campus local time (UTC+1) later than 08:15:00.
        # We compare the EVENT time (eventTime), not the time Spark receives the record.
        late = (
            parsed.filter(col("eventType") == "check-in")
            .withColumn("campus_time", from_utc_timestamp(col("event_ts"), CAMPUS_TZ))
            .filter(date_format(col("campus_time"), "HH:mm:ss") > LATE_AFTER)
            .select("studentId", "department", "eventTime", "campus_time")
        )
        queries.append(start_query(late, "append", "late", args))

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
