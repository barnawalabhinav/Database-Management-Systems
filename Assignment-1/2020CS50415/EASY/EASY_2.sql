SELECT num_adm.subject_id,
    num_adm.num_admissions
FROM (
        SELECT subject_id,
            COUNT(*) AS num_admissions
        FROM admissions
        GROUP BY subject_id
    ) AS num_adm
    INNER JOIN (
        SELECT MAX(num_admissions) AS max_admissions
        FROM (
                SELECT COUNT(*) AS num_admissions
                FROM admissions
                GROUP BY subject_id
            ) AS nums
    ) AS max_adm ON max_adm.max_admissions = num_adm.num_admissions
ORDER BY subject_id ASC;