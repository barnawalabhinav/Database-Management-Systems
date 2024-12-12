SELECT subject_id,
    anchor_age
FROM patients
WHERE anchor_age < 50
    AND subject_id IN (
        SELECT subject_id
        FROM procedures_icd
        GROUP BY icd_code,
            icd_version,
            subject_id
        HAVING COUNT(DISTINCT hadm_id) > 1
    )
ORDER BY subject_id ASC,
    anchor_age ASC;