-- impute_actuations.sql
-- Ensure alternating on/off events to track detector states correctly.
-- Two consecutive on/off events may occur due to missing data or extension time.
-- The first event must be an on event to avoid negative numbers.

WITH lagged AS (
    SELECT
        *,
        LAG(EventId) OVER (PARTITION BY DeviceId, Detector, DetectorType ORDER BY TimeStamp) AS PrevEventId,
        DATEDIFF('MILLISECOND', LAG(TimeStamp) OVER (PARTITION BY DeviceId, Detector, DetectorType ORDER BY TimeStamp), TimeStamp) AS PrevDiff
    FROM raw_data
),
interpolate_OFFS_rapid AS (
    SELECT TimeStamp, DeviceId, 81 AS EventId, Detector, DetectorType
    FROM lagged
    WHERE PrevDiff <= 2000
        AND EventId = 82
        AND PrevEventId = 82
),
interpolate_OFFs AS (
    SELECT
        TimeStamp - INTERVAL (PrevDiff / 2) MILLISECOND AS TimeStamp,
        DeviceId,
        81 AS EventId,
        Detector,
        DetectorType
    FROM lagged
    WHERE PrevDiff > 2000
        AND EventId = 82
        AND PrevEventId = 82
),
interpolate_ONs AS (
    SELECT TimeStamp - INTERVAL (PrevDiff / 2) MILLISECOND AS TimeStamp,
        DeviceId,
        82 AS EventId,
        Detector,
        DetectorType
    FROM lagged
    WHERE EventId = 81 AND PrevEventId = 81
),
interpolate_PED_OFFS AS (
    SELECT TimeStamp - INTERVAL (PrevDiff / 2) MILLISECOND AS TimeStamp,
        DeviceId,
        89 AS EventId,
        Detector,
        DetectorType
    FROM lagged
    WHERE EventId = 90 AND PrevEventId = 90
),
interpolate_PED_ONs AS (
    SELECT TimeStamp - INTERVAL (PrevDiff / 2) MILLISECOND AS TimeStamp,
        DeviceId,
        90 AS EventId,
        Detector,
        DetectorType
    FROM lagged
    WHERE EventId = 89 AND PrevEventId = 89
),
combined AS (
    SELECT * FROM raw_data
    UNION ALL
    SELECT * FROM interpolate_OFFS_rapid
    UNION ALL
    SELECT * FROM interpolate_OFFs
    UNION ALL
    SELECT * FROM interpolate_ONs
    UNION ALL
    SELECT * FROM interpolate_PED_OFFS
    UNION ALL
    SELECT * FROM interpolate_PED_ONs
),
ordered_rows AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY DeviceId, Detector, DetectorType ORDER BY TimeStamp) AS row_num
    FROM combined
)
SELECT TimeStamp,
    DeviceId,
    EventId,
    Detector,
    DetectorType
FROM ordered_rows
WHERE NOT (row_num = 1 AND EventId IN (81, 89))
ORDER BY TimeStamp