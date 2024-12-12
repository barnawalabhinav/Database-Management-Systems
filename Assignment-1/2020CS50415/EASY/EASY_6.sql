SELECT COUNT(hadm_id)
FROM diagnoses_icd
    INNER JOIN (
        SELECT icd_code,
            icd_version
        FROM d_icd_procedures
        WHERE long_title = 'Cholera due to vibrio cholerae'
    ) AS cholera_diag ON diagnoses_icd.icd_code = cholera_diag.icd_code
    AND diagnoses_icd.icd_version = cholera_diag.icd_version;