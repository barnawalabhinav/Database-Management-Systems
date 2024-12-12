WITH final_tab AS (
    SELECT d_icd_diagnoses.long_title,
        ROUND(
            AVG(
                CASE
                    WHEN admissions.hospital_expire_flag = 0 THEN patients.anchor_age
                END
            ),
            2
        ) AS survived_avg_age,
        SUM(
            CASE
                WHEN admissions.hospital_expire_flag = 1 THEN 1
                ELSE 0
            END
        ) * 100.0 / COUNT(*) AS mortality_rate
    FROM d_icd_diagnoses
        INNER JOIN diagnoses_icd ON diagnoses_icd.icd_code = d_icd_diagnoses.icd_code
        AND diagnoses_icd.icd_version = d_icd_diagnoses.icd_version
        INNER JOIN admissions ON admissions.subject_id = diagnoses_icd.subject_id
        AND admissions.hadm_id = diagnoses_icd.hadm_id
        INNER JOIN patients ON admissions.subject_id = patients.subject_id
    GROUP BY d_icd_diagnoses.icd_code,
        d_icd_diagnoses.icd_version
    ORDER BY mortality_rate DESC
    LIMIT 245
)
SELECT long_title,
    survived_avg_age
FROM final_tab
WHERE mortality_rate < 100.0
ORDER BY long_title ASC,
    survived_avg_age DESC;