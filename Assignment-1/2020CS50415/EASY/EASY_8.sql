SELECT subject_id,
    anchor_age
FROM patients
WHERE subject_id IN (
        SELECT subject_id
        FROM icustays
        WHERE hadm_id IN (
                SELECT hadm_id
                FROM diagnoses_icd
                    INNER JOIN (
                        SELECT icd_code,
                            icd_version
                        FROM d_icd_diagnoses
                        WHERE long_title = 'Typhoid fever'
                    ) AS typhic ON diagnoses_icd.icd_code = typhic.icd_code
                    AND diagnoses_icd.icd_version = typhic.icd_version
            )
    )
ORDER BY subject_id ASC,
    anchor_age ASC;