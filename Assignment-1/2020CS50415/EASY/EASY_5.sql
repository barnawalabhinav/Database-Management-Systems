SELECT a.subject_id,
    a.anchor_age,
    a.count
FROM (
        SELECT patients.subject_id,
            patients.anchor_age,
            pat_Freq.count
        FROM patients
            INNER JOIN (
                SELECT subject_id,
                    COUNT(*)
                FROM icustays
                WHERE first_careunit = 'Coronary Care Unit (CCU)'
                GROUP BY subject_id
            ) AS pat_Freq ON pat_Freq.subject_id = patients.subject_id
    ) AS a
    INNER JOIN (
        SELECT MAX(pat_Freq.count) AS max_count
        FROM (
                SELECT COUNT(*)
                FROM icustays
                WHERE first_careunit = 'Coronary Care Unit (CCU)'
                GROUP BY subject_id
            ) AS pat_Freq
    ) AS b ON b.max_count = a.count
ORDER BY anchor_age DESC,
    subject_id DESC;