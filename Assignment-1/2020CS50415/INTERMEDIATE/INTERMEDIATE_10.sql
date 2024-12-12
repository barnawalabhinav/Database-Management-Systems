SELECT patients.subject_id,
    patients.anchor_year,
    drug_info.drug
FROM patients
    INNER JOIN (
        SELECT subject_id,
            drug
        FROM prescriptions
        GROUP BY subject_id,
            drug
        HAVING COUNT(DISTINCT hadm_id) > 1
    ) AS drug_info ON patients.subject_id = drug_info.subject_id
ORDER BY subject_id DESC,
    anchor_year DESC,
    drug DESC
LIMIT 1000;