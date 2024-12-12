SELECT drug,
    COUNT(hadm_id) AS prescription_count
FROM (
        SELECT admissions.hadm_id,
            prescriptions.drug
        FROM admissions
            INNER JOIN prescriptions ON (
                admissions.hadm_id = prescriptions.hadm_id
                AND EXTRACT(
                    epoch
                    FROM (prescriptions.starttime - admissions.admittime)
                ) BETWEEN 0 AND 43200
            )
    ) AS drug_table
GROUP BY drug
ORDER BY prescription_count DESC,
    drug DESC
LIMIT 1000;