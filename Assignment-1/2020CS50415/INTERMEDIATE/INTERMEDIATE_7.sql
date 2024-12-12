SELECT cnt.subject_id,
    cnt.total_stays,
    cnt.avg_length_of_stay
FROM (
        SELECT subject_id,
            COUNT(DISTINCT stay_id) AS total_stays,
            AVG(los) AS avg_length_of_stay
        FROM icustays
        WHERE los IS NOT NULL
            AND stay_id IS NOT NULL
        GROUP BY subject_id
        HAVING COUNT(DISTINCT stay_id) > 4
    ) AS cnt
    INNER JOIN (
        SELECT DISTINCT subject_id
        FROM icustays
        WHERE first_careunit LIKE '%MICU%'
            OR last_careunit LIKE '%MICU%'
    ) AS micu ON cnt.subject_id = micu.subject_id
ORDER BY avg_length_of_stay DESC,
    total_stays DESC,
    subject_id DESC
LIMIT 500;