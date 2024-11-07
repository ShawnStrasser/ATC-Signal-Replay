-- load_conflict_events.sql
-- Vehicle Phases
WITH
Phase AS (
SELECT
    TimeStamp,
    EventTypeID AS EventId,
    CONCAT('Ph', Parameter) AS Parameter,
    CASE WHEN EventTypeID = 1 THEN 1 ELSE 0 END AS state_integer
FROM Event
WHERE EventTypeID IN (1,10) -- Phase begin green, begin red
),
Overlap AS (
SELECT
    TimeStamp,
    EventTypeID AS EventId,
    CONCAT('O', Parameter) AS Parameter,
    CASE WHEN EventTypeID IN(61,63) THEN 1 ELSE 0 END AS state_integer
FROM Event
WHERE EventTypeID IN (61,63,65) -- Overlap begin green, yellow, end
),
Ped AS (
SELECT
    TimeStamp,
    EventTypeID AS EventId,
    CONCAT('Ped', Parameter) AS Parameter,
    CASE WHEN EventTypeID = 21 THEN 1 ELSE 0 END AS state_integer
FROM Event
WHERE EventTypeID IN (21,23) -- Pedestrian begin walk, begin don't walk
),
OPed AS (
SELECT
    TimeStamp,
    EventTypeID AS EventId,
    CONCAT('OPed', Parameter) AS Parameter,
    CASE WHEN EventTypeID = 67 THEN 1 ELSE 0 END AS state_integer
FROM Event
WHERE EventTypeID IN (67, 65) -- Overlap Pedestrian begin walk, overlap end
)
SELECT * FROM Phase
UNION ALL
SELECT * FROM Overlap
UNION ALL
SELECT * FROM Ped
UNION ALL
SELECT * FROM OPed
ORDER BY TimeStamp, EventId DESC;