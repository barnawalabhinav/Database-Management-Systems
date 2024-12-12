WITH b AS (
    SELECT subject_id,
        hadm_id
    FROM diagnoses_icd
        INNER JOIN (
            SELECT icd_code,
                icd_version
            FROM d_icd_diagnoses
            WHERE d_icd_diagnoses.long_title LIKE '%Meningitis%'
        ) AS meningitis ON diagnoses_icd.icd_code = meningitis.icd_code
        AND diagnoses_icd.icd_version = meningitis.icd_version
)
SELECT patients.gender,
    ROUND(
        SUM(
            CASE
                WHEN big_a.hospital_expire_flag = 1 THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*),
        2
    ) AS mortality_rate
FROM b
    INNER JOIN admissions big_a ON b.hadm_id = big_a.hadm_id
    AND b.subject_id = big_a.subject_id
    AND big_a.admittime = (
        SELECT MAX(admittime)
        FROM admissions
        WHERE subject_id = big_a.subject_id
    )
    INNER JOIN patients ON big_a.subject_id = patients.subject_id
GROUP BY gender
ORDER BY mortality_rate ASC,
    gender DESC;