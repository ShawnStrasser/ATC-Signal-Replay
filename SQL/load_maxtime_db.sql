-- load .db log from maxtime
SELECT
    TO_TIMESTAMP(Timestamp + (Tick / 10))::TIMESTAMP AS TimeStamp,
    0 AS DeviceId,
    EventTypeID AS EventId,
    Parameter AS Detector,
    CASE
        WHEN EventTypeID IN(81,82) THEN 'Vehicle'
        WHEN EventTypeID IN(104,102) THEN 'Preempt'
        WHEN EventTypeID IN(89,90) THEN 'Ped'
    END AS DetectorType
FROM Event
WHERE EventId IN(81,82, 104,102, 89,90) -- OFF/ON for vehicle detector, preempt, ped detector
    AND Parameter < 65 -- Filter out dummy detectors
ORDER BY Timestamp