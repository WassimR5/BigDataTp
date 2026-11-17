# TP06 — Real-Time Data Processing Using Apache Spark (Structured Streaming and Flink)

**University of Eloued — Department of Computer Science**
**Module:** Big Data Analytics — Master 2 IA & Data Science — 2026/2027
**Student:** _<your full name>_

---

## 1. Description

Synthetic campus check-in events are sent to the Kafka topic `campus-events` (`producer.py`).
Spark Structured Streaming (`spark_stream.py`) reads them from Kafka, parses the JSON, keeps the
check-ins, counts them per department in one-minute **event-time** windows, and finds late check-ins
(after 08:15 campus time). `flink_window.sql` is the Flink SQL version of the window query (analysis).

## 2. Versions used

_Fill with your real versions (`spark-submit --version`, `java -version`, `python --version`)._

| Component | Version |
|---|---|
| Python | _<...>_ |
| Java | _<...>_ |
| PySpark / Spark | 3.5.3 |
| Scala (connector) | 2.12 |
| Kafka connector | `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3` |
| Kafka (TP05 broker) | apache/kafka 3.9.1 |
| Flink | not executed (query analysis only) |

## 3. Files

| File | Role |
|---|---|
| `producer.py` | Sends 7 events (one out of order). Options: `--campus-time`, `--extra`, `--count`, `--interval` |
| `spark_stream.py` | Spark application. Options for the experiments: `--trigger`, `--window`, `--slide`, `--watermark`, `--mode` |
| `flink_window.sql` | Flink SQL table + tumbling-window query + analysis |
| `requirements.txt` | `kafka-python-ng`, `pyspark` |
| `screenshots/` | Evidence |

## 4. How to run

```bash
# 1. Kafka from TP05 must be running
cd ~/Downloads/tp05-kafka && docker compose up -d && cd -

# 2. Create and describe the topic
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-events --partitions 3 --replication-factor 1
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-events

# 3. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 4. Terminal 1: start Spark FIRST
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream.py

# 5. Terminal 2: send the events
python producer.py
```

If `spark-submit` is not installed, `pip install pyspark` provides it inside the venv.
Late check-ins (Task 5): `python producer.py --campus-time 08:20` and `spark_stream.py --mode late`.

## 5. Results (fill after running)

**Kafka topic** (`screenshots/kafka_topic.png`): _<paste: 3 partitions, replication factor 1>_

**Producer** (`screenshots/producer_output.png`): _<7 events sent>_

**Spark output** (`screenshots/spark_output.png`): _<check-ins per department and window>_

**Task 5 — late check-ins:** timezone used = campus local time (UTC+1, Africa/Algiers). A check-in is late
when its local time is later than 08:15:00. Comparing the **event time** is correct because a record can
arrive late (network delay, replay) and the receiving time would then give a wrong answer. _<result>_

### Task 4 experiments (change one setting at a time)

| Exp. | Change | Observation |
|---|---|---|
| A | `--trigger 5` | _<...>_ |
| B | `--window "2 minutes"` | _<...>_ |
| C | `--window "2 minutes" --slide "1 minute"` | _<...>_ |
| D | `--watermark "2 minutes"` | _<...>_ |
| E | Ctrl+C then restart with the same checkpoint | _<...>_ (`screenshots/checkpoint_or_recovery.png`) |

**Reminders for the explanations:**
- A: the trigger sets how often Spark starts a micro-batch, so an event waits for the next batch (up to 5 s), not exactly 5 s.
- B: bigger windows group more events, so fewer output rows.
- C: windows overlap (2 min long, every 1 min), so one event belongs to two windows.
- D: longer watermark waits longer for late events but results are final later.
- E: Spark continues from the saved offsets and state, so events already processed are not counted again.

## 6. Answers to the report questions

**1. Role of Kafka vs Spark/Flink.** Kafka transports and stores the events (producers, consumers, topics, partitions). Spark/Flink process them: parse, filter, aggregate by time window, and produce results.

**2. How Spark represents a stream.** As an unbounded table where new events are new rows appended. A query on it is run incrementally: Spark processes the new rows and updates the result (state), instead of recomputing everything.

**3. Micro-batch.** A small group of events collected and processed together as one job. Advantage: high throughput and fault tolerance. Trade-off: latency of the batch interval (not instant processing).

**4. `startingOffsets` and `subscribe`.** `subscribe` chooses the topic to read (`campus-events`). `startingOffsets` chooses where to start the first time: `earliest` = from the beginning of the topic, `latest` = only new events. After a restart with a checkpoint, Spark continues from the saved offsets.

**5. Why define the JSON schema.** Kafka gives bytes/strings with no structure. Spark must know field names and types to parse them with `from_json`; a fixed schema is also faster and avoids type guessing.

**6. Event time vs processing time (example).** Event S512 has `eventTime` = base + 12 s but is sent as the 7th event, after events with eventTime up to +48 s. Event time = when the check-in happened (+12 s). Processing time = when Spark handles it (later). The window uses the event time, so S512 still counts in its correct window.

**7. Watermark.** A threshold (here 30 s) saying how late events may be compared with the largest event time seen; Spark uses it to decide when old windows can be closed and their state removed. It does **not** guarantee that no late event is lost: events later than the watermark can be dropped.

**8. Windows.** Tumbling: fixed size, no overlap (every event in one window). Sliding: fixed size but overlapping, advancing by a slide (an event in several windows). Session: dynamic windows that close after a gap of inactivity (for example 5 minutes without events from the same student).

**9. Checkpoint.** It saves the progress (Kafka offsets), the state of aggregations and query metadata. The location must stay the same so Spark finds this saved information and recovers without losing or recounting events; a new folder starts the query from scratch.

**10. Spark vs Flink (two differences).** Spark Structured Streaming works in micro-batches (latency of seconds), Flink processes event by event (lower latency). Flink has more native event-time and state handling for streaming; Spark is strong in a unified batch + streaming ecosystem (SQL, MLlib).

**11. Choice for the smart campus.** Spark Structured Streaming: attendance monitoring tolerates a few seconds of latency, it handles event-time windows and watermarks, it keeps the state with checkpoints, it has a rich ecosystem, and the team already knows Python and PySpark. Flink would be preferred if very low latency or complex stateful event processing were needed.

**12. Evidence.** See `screenshots/` (Kafka topic, producer, Spark output, experiments) and `flink_window.sql`.

**Flink SQL questions (Task 6):** (1) event time = `event_ts` (from `eventTime`); (2) watermark = accepted lateness of 30 s, a window closes when the watermark passes its end; (3) 1 minute; (4) `window_start` and `window_end` show which interval each count belongs to.

## 7. Problems and solutions

| Problem | Solution |
|---|---|
| _<problem>_ | _<solution>_ |

Possible ones: Java missing or incompatible with Spark; Kafka connector package not downloaded (needs internet, version must match 3.5.3 / Scala 2.12); `NoBrokersAvailable` when Kafka is not running; Spark shows nothing because the producer ran before Spark with the wrong `startingOffsets`.
