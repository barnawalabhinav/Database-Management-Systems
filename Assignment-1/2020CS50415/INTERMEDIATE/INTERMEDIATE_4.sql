SELECT urgent_death_icd.subject_id,
    urgent_death_icd.hadm_id,
    urgent_death_icd.icd_code,
    d_icd_diagnoses.long_title
FROM (
        SELECT diagnoses_icd.subject_id,
            diagnoses_icd.hadm_id,
            diagnoses_icd.icd_code,
            diagnoses_icd.icd_version
        FROM diagnoses_icd
            INNER JOIN (
                SELECT subject_id,
                    hadm_id
                FROM admissions
                WHERE admission_type = 'URGENT'
                    AND hospital_expire_flag = 1
            ) AS urgent_deaths ON diagnoses_icd.subject_id = urgent_deaths.subject_id
            AND diagnoses_icd.hadm_id = urgent_deaths.hadm_id
    ) AS urgent_death_icd
    INNER JOIN d_icd_diagnoses ON urgent_death_icd.icd_code = d_icd_diagnoses.icd_code
    AND urgent_death_icd.icd_version = d_icd_diagnoses.icd_version
WHERE d_icd_diagnoses.long_title IS NOT NULL
ORDER BY subject_id DESC,
    hadm_id DESC,
    icd_code DESC,
    long_title DESC
LIMIT 1000;