-- load_conflict_events.sql
SELECT
    TimeStamp,
    EventTypeID AS EventId,
    CONCAT(
        CASE WHEN EventTypeID IN (1,10) THEN 'Ph' ELSE 'O' END,
        Parameter
    ) AS Parameter,
    CASE WHEN EventTypeID IN (1,61,63) THEN 1 ELSE 0 END AS state_integer
FROM Event
WHERE EventTypeID IN (1,10,61,63,65) -- Phase/Overlap begin green, yellow, red
ORDER BY TimeStamp, EventTypeID DESC