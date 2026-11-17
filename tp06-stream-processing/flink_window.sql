-- TP06 Task 6 - Flink SQL: event-time tumbling window on the Kafka topic campus-events.
-- Query analysis (the optional execution needs a Flink SQL environment and a Kafka connector
-- JAR matching the exact Flink version; not executed in this lab).

-- 1) Source table. The JSON key is "eventTime" (an ISO string such as 2026-11-17T08:20:02+00:00),
--    so it is read as STRING and converted to a TIMESTAMP(3) computed column called event_ts.
CREATE TABLE campus_events (
    eventId    INT,
    studentId  STRING,
    eventType  STRING,
    department STRING,
    eventTime  STRING,
    event_ts AS TO_TIMESTAMP(REPLACE(SUBSTR(eventTime, 1, 19), 'T', ' ')),
    -- Watermark: Flink accepts events up to 30 seconds late relative to the largest event_ts seen.
    WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'campus-events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'tp06-flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
);

-- 2) Check-ins per department in 1-minute tumbling event-time windows.
SELECT
    window_start,
    window_end,
    department,
    COUNT(*) AS checkins
FROM TABLE(
    TUMBLE(
        TABLE campus_events,
        DESCRIPTOR(event_ts),
        INTERVAL '1' MINUTE
    )
)
WHERE eventType = 'check-in'
GROUP BY window_start, window_end, department;

-- Answers (query analysis)
-- (1) Event time is defined by the column event_ts (built from the field eventTime); it is the
--     time attribute named in WATERMARK FOR and in DESCRIPTOR(event_ts).
-- (2) WATERMARK FOR event_ts AS event_ts - INTERVAL '30' SECOND means: Flink considers that no
--     event older than (largest event_ts - 30 s) will normally arrive. A window is closed and its
--     result emitted when the watermark passes window_end; later events are dropped.
-- (3) Each tumbling window lasts 1 minute (INTERVAL '1' MINUTE); windows do not overlap.
-- (4) window_start and window_end tell which time interval each count belongs to, so results can
--     be read, compared, and plotted on a dashboard.
