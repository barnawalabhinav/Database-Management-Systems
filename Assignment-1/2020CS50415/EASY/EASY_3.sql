SELECT COUNT(*)
FROM labevents
WHERE priority = 'ROUTINE'
    AND flag = 'abnormal';