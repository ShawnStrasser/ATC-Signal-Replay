-- load .db log from maxtime
-- ninja2 template with variables in curly braces
SELECT
    {{timestamp}}::TIMESTAMP AS TimeStamp,
    device_id as DeviceId,
    {{eventid}}::int AS EventId,
    {{parameter}}::int AS Detector,
    CASE
        WHEN {{eventid}} IN(81,82) THEN 'Vehicle'
        WHEN {{eventid}} IN(104,102) THEN 'Preempt'
        WHEN {{eventid}} IN(89,90) THEN 'Ped'
    END AS DetectorType
FROM '{{from_path}}'
WHERE EventId IN(81,82, 104,102, 89,90) -- OFF/ON for vehicle detector, preempt, ped detector
    AND Parameter < 65 -- Filter out dummy detectors
ORDER BY Timestamp