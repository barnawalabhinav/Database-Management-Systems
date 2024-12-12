WITH proc_sum AS (
    SELECT proc_info.icd_code,
        proc_info.icd_version,
        proc_info.subject_id,
        proc_info.hadm_id,
        SUM(COALESCE(icustays.los, 0)) AS sum_stay
    FROM (
            SELECT DISTINCT procedures_icd.icd_code,
                procedures_icd.icd_version,
                procedures_icd.subject_id,
                procedures_icd.hadm_id
            FROM procedures_icd
        ) AS proc_info
        LEFT OUTER JOIN icustays ON proc_info.subject_id = icustays.subject_id
        AND proc_info.hadm_id = icustays.hadm_id
    GROUP BY proc_info.icd_code,
        proc_info.icd_version,
        proc_info.subject_id,
        proc_info.hadm_id
)
SELECT proc_min.subject_id,
    patients.gender,
    proc_min.icd_code,
    proc_min.icd_version
FROM (
        SELECT icd_code,
            icd_version,
            subject_id,
            MIN(sum_stay) AS min_stay
        FROM proc_sum
        GROUP BY icd_code,
            icd_version,
            subject_id
    ) AS proc_min
    INNER JOIN (
        SELECT proc_sum.icd_code,
            proc_sum.icd_version,
            AVG(COALESCE(sum_stay, 0)) AS avg_stay
        FROM proc_sum
        GROUP BY icd_code,
            icd_version
    ) AS proc_avg ON proc_min.min_stay < proc_avg.avg_stay
    AND proc_min.icd_code = proc_avg.icd_code
    AND proc_min.icd_version = proc_avg.icd_version
    INNER JOIN patients ON proc_min.subject_id = patients.subject_id
ORDER BY subject_id ASC,
    icd_code DESC,
    icd_version DESC,
    gender ASC
LIMIT 1000;