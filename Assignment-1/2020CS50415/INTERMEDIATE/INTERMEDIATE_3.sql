SELECT subject_id,
    COUNT(description) AS diagnoses_count
FROM drgcodes
WHERE UPPER(description) LIKE UPPER('%ALCOHOLIC%')
    AND subject_id IN (
        SELECT subject_id
        FROM (
                SELECT subject_id
                FROM drgcodes
                WHERE UPPER(description) LIKE UPPER('%ALCOHOLIC%')
                GROUP BY subject_id,
                    hadm_id,
                    description
            ) AS dist_adm_desc
        GROUP BY subject_id
        HAVING COUNT(*) > 1
    )
GROUP BY subject_id
ORDER BY diagnoses_count DESC,
    subject_id DESC;