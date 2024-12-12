SELECT adm_info.subject_id,
    patients.gender,
    adm_info.total_admissions,
    adm_info.last_admission,
    adm_info.first_admission,
    adm_info.diagnosis_count
FROM (
        SELECT diag_counts.subject_id,
            adm_counts.total_admissions,
            adm_counts.last_admission,
            adm_counts.first_admission,
            diag_counts.diagnosis_count
        FROM (
                SELECT subject_id,
                    COUNT(*) AS diagnosis_count
                FROM diagnoses_icd
                WHERE icd_code = '5723'
                GROUP BY subject_id
                HAVING COUNT(*) > 0
            ) AS diag_counts
            INNER JOIN (
                SELECT subject_id,
                    count(DISTINCT hadm_id) AS total_admissions,
                    MIN(admittime) AS first_admission,
                    MAX(admittime) AS last_admission
                FROM admissions
                GROUP BY subject_id
            ) AS adm_counts ON diag_counts.subject_id = adm_counts.subject_id
    ) AS adm_info
    INNER JOIN patients ON adm_info.subject_id = patients.subject_id
ORDER BY adm_info.total_admissions DESC,
    adm_info.diagnosis_count DESC,
    adm_info.last_admission DESC,
    adm_info.first_admission DESC,
    patients.gender DESC,
    adm_info.subject_id DESC
LIMIT 1000;