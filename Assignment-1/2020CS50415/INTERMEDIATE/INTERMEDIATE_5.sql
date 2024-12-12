SELECT icu_table.subject_id,
    icu_table.avg_stay_duration
FROM (
        SELECT subject_id,
            hadm_id,
            AVG(los) AS avg_stay_duration
        FROM icustays
        WHERE los IS NOT NULL
        GROUP BY subject_id,
            hadm_id
    ) AS icu_table
    INNER JOIN (
        SELECT DISTINCT subject_id,
            hadm_id
        FROM labevents
        WHERE itemid = 50878
            AND hadm_id IS NOT NULL
    ) AS lab_table ON icu_table.subject_id = lab_table.subject_id
    AND icu_table.hadm_id = lab_table.hadm_id
ORDER BY avg_stay_duration DESC,
    subject_id DESC
LIMIT 1000;