SELECT a.pharmacy_id,
    a.num_patients_visited
FROM (
        SELECT pharmacy_id,
            COUNT(DISTINCT subject_id) AS num_patients_visited
        FROM prescriptions
        GROUP BY pharmacy_id
    ) AS a
    INNER JOIN (
        SELECT MIN(num_patients_visited) AS min_num_patients_visited
        FROM (
                SELECT COUNT(DISTINCT subject_id) AS num_patients_visited
                FROM prescriptions
                GROUP BY pharmacy_id
            ) AS nums
    ) AS min_a ON min_a.min_num_patients_visited = a.num_patients_visited
ORDER BY pharmacy_id ASC;