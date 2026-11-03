# TP05 — Real-Time Data Streaming Using Apache Kafka

**University of Eloued — Department of Computer Science**
**Module:** Big Data Analytics — Master 2 IA & Data Science — 2026/2027
**Student:** Zid Ouassim

---

## 1. Description

Simulated campus sensors (`producer.py`) send temperature and humidity readings as JSON events to a
Kafka topic called `campus-sensors`. A consumer (`consumer.py`) reads the events in real time and
prints an alert when the temperature is **35 °C or higher**. Kafka runs in Docker (KRaft mode, one broker).

## 2. Files

| File | Role |
|---|---|
| `compose.yaml` | Starts a single Kafka broker on `localhost:9092` |
| `requirements.txt` | Python dependency (`kafka-python`) |
| `producer.py` | Sends 30 events, one per second |
| `consumer.py` | Reads events and prints an alert for T ≥ 35 °C |
| `extras/` | Optional scripts: running average, group id, building filter, CSV export |
| `screenshots/` | Evidence |

## 3. How to run

```bash
# 1. Start Kafka
docker compose up -d
docker compose ps

# 2. Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 3. Create and describe the topic
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors

# 4. Run (two terminals, venv activated in both)
python consumer.py     # terminal 1 (start first)
python producer.py     # terminal 2

# 5. List consumer groups
docker exec -it tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# 6. Cleanup
docker compose down
```

> On Python 3.12+, `kafka-python==2.0.2` does not work. I used `kafka-python-ng` (same `import kafka`):
> `pip install kafka-python-ng`

## 4. Results

**Kafka is running.** Container `tp05-kafka` (image `apache/kafka:3.9.1`) is `Up`, port 9092 is open
(screenshot 01).

**Topic description** (screenshot 02): `PartitionCount: 3`, `ReplicationFactor: 1`.

| Partition | Leader | Replicas | Isr |
|---|---|---|---|
| 0 | 1 | 1 | 1 |
| 1 | 1 | 1 | 1 |
| 2 | 1 | 1 | 1 |

- **3 partitions:** the topic is split into three ordered logs, so events can be spread and read in parallel.
- **Replication factor 1:** each partition has only one copy, because there is only one broker (no fault tolerance).

**Producer** (screenshot 03): 30 events were sent, one per second (09:42:47 → 09:43:16 UTC). Example:

```
Sent: {'event_id': 1, 'sensor_id': 'sensor-02', 'temperature_c': 41.54, 'humidity_pct': 30.31, 'timestamp': '2026-11-03T09:42:47.609724+00:00'}
```

**Consumer** (screenshot 04): all 30 events were received in real time. 7 events triggered
`ALERT: High temperature detected!`:

| Event | Sensor | Temperature |
|---|---|---|
| 1 | sensor-02 | 41.54 °C |
| 4 | sensor-04 | 35.55 °C |
| 7 | sensor-04 | 41.21 °C |
| 12 | sensor-01 | 41.09 °C |
| 15 | sensor-01 | 38.98 °C |
| 17 | sensor-02 | 41.59 °C |
| 24 | sensor-03 | 38.82 °C |

Running average temperature of the 30 events (`extras/consumer_extra.py`): **29.39 °C**.

**Consumer groups** (screenshot 05): the list shows `tp05-campus-monitor` and `tp05-second-group`.

**Second consumer group:** after the producer had finished, a new consumer with another group id
(`tp05-second-group`) read **all 30 events again from the beginning**, because a new group has no saved
offsets and `auto_offset_reset='earliest'` applies. The events were spread over the partitions as
**partition 0: 7, partition 1: 11, partition 2: 12** (total 30), because the producer sends no key.
Order is kept inside a partition only (see the left window of screenshot 04).

## 5. Answers to the questions

**1. What problem does Kafka solve?**
It lets many applications send and read streams of events in real time through a shared, durable log.
Producers and consumers are independent, the volume can be high, and events are kept so they can be read again.

**2. Definitions**
- **Producer:** application that sends events to Kafka.
- **Consumer:** application that reads events from Kafka.
- **Broker:** Kafka server that stores the data and serves producers and consumers.
- **Topic:** named stream of events (here `campus-sensors`).
- **Partition:** an ordered log that stores a part of a topic.
- **Offset:** position number of an event inside a partition.
- **Consumer group:** consumers with the same `group_id` that share the partitions of a topic.

**3. Topic vs partition?**
A topic is the logical name of a stream. A partition is a physical part of the topic (an ordered log).
A topic has one or more partitions; order is guaranteed only inside one partition.

**4. Why `auto_offset_reset='earliest'`? When does it apply?**
It makes the consumer start from the oldest event. It applies only when the group has no saved offset
(first run of a new `group_id`). After that, the consumer continues from its last committed offset.

**5. Purpose of the group id? What if a new one is used?**
It identifies the group: Kafka shares partitions between its members and stores its offsets.
A new group id has no offsets, so it reads the topic again from the beginning (confirmed with `tp05-second-group`:
30 events). Consumers in the same group share the events; different groups each get all of them.

**6. Why JSON? One limitation.**
JSON is simple, readable and easy to parse in Python. Limitation: it is verbose and has no enforced
schema, so field names or types can change without any error.

**7. How does Docker Compose simplify deployment?**
One file describes the whole service, and `docker compose up -d` starts it with one command. No manual
installation of Java or Kafka is needed, and the setup is reproducible on any machine.

**8. Limitations of this single-broker setup**
- One broker: if it stops, the service stops (no fault tolerance).
- Replication factor 1: no backup copy of the data.
- All partitions are on one machine: no real scalability.
- No security (PLAINTEXT, no authentication) and no monitoring.
- Only clients on the same machine can connect (`localhost`).

**9. Evidence:** see section 6.

## 6. Screenshots

| File | Content |
|---|---|
| `screenshots/01-docker-compose-ps.png` | Running container |
| `screenshots/02-topic-describe.png` | Topic description |
| `screenshots/03-producer-output.png` | Producer output (30 events) |
| `screenshots/04-consumer-output.png` | Consumer output with alerts (right) and second group (left) |
| `screenshots/05-consumer-groups.png` | List of consumer groups |

## 7. Problems and solutions

| Problem | Solution |
|---|---|
| `python: command not found` | On this Linux system the command is `python3`; after activating the venv, `python` works. |
| `error: externally-managed-environment` when using `pip` | `pip` was used outside the virtual environment. Activate it with `source .venv/bin/activate`. |
| `kafka-python==2.0.2` is not compatible with Python 3.12 | Installed `kafka-python-ng` (version 2.2.3) in the venv. |
| Terminal showed `>` and waited | A `{` was pasted by mistake and the shell waited for `}`. Fixed with `Ctrl+C`. |
