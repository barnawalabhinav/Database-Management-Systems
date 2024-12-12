SELECT diag_info.subject_id,
    diag_info.hadm_id,
    diag_info.distinct_diagnoses_count,
    drug_info.drug
FROM (
        SELECT subject_id,
            hadm_id,
            COUNT(DISTINCT icd_code) AS distinct_diagnoses_count
        FROM diagnoses_icd
        WHERE STARTS_WITH(icd_code, 'V4')
        GROUP BY subject_id,
            hadm_id
        HAVING COUNT(DISTINCT icd_code) > 1
    ) AS diag_info
    INNER JOIN (
        select prescriptions.subject_id,
            prescriptions.hadm_id,
            prescriptions.drug
        FROM prescriptions
        WHERE UPPER(prescriptions.drug) LIKE UPPER('%prochlorperazine%')
            OR UPPER(prescriptions.drug) LIKE UPPER('%bupropion%')
    ) AS drug_info ON drug_info.subject_id = diag_info.subject_id
    AND drug_info.hadm_id = diag_info.hadm_id
ORDER BY distinct_diagnoses_count DESC,
    subject_id DESC,
    hadm_id DESC,
    drug ASC;