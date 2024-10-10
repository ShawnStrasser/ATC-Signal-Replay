-- generate_activation_feed.sql
WITH DetectorEvents AS (
    SELECT
        TimeStamp,
        DeviceId,
        Detector,
        EventId,
        DetectorType,
        ((Detector - 1) // 8)::INT + 1 AS group_number,
        CASE
            WHEN EventId IN (82,90) THEN POW(2, (Detector - 1) % 8)
            WHEN EventId IN (81,89) THEN -POW(2, (Detector - 1) % 8)
            ELSE 0
        END AS value_change
    FROM imputed
),
CumulativeState AS (
    SELECT
        TimeStamp,
        DeviceId,
        Detector,
        EventId,
        DetectorType,
        group_number,
        SUM(value_change) OVER (PARTITION BY DeviceId, group_number, DetectorType ORDER BY TimeStamp, Detector)::INT AS state_integer
    FROM DetectorEvents
),
PreemptEvents AS (
    SELECT
        TimeStamp,
        DeviceId,
        Detector,
        EventId,
        DetectorType,
        CASE
            WHEN EventID IN (104,102) THEN Detector
            ELSE group_number
        END AS group_number,
        CASE
            WHEN EventId = 102 THEN 1
            WHEN EventId = 104 THEN 0
            ELSE state_integer
        END AS state_integer
    FROM CumulativeState
),
RankedEvents AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY TimeStamp, DeviceId, group_number, DetectorType ORDER BY Detector DESC) AS rn
    FROM PreemptEvents
)
SELECT
    TimeStamp,
    COALESCE(EXTRACT(EPOCH FROM (TimeStamp - LAG(TimeStamp) OVER (ORDER BY TimeStamp))), 0) AS sleep_time,
    DeviceId,
    group_number,
    DetectorType,
    state_integer
FROM RankedEvents
WHERE rn = 1
ORDER BY TimeStamp, DeviceId, group_number