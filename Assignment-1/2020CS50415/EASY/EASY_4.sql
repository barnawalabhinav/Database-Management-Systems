SELECT COUNT(*)
FROM (
        SELECT DISTINCT icd_code,
            icd_version
        FROM procedures_icd
        WHERE subject_id = '10000117'
    ) AS patients;