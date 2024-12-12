SELECT subject_id,
    COUNT(stay_id)
FROM icustays
GROUP BY subject_id
HAVING COUNT(stay_id) >= 5
ORDER BY COUNT(stay_id) DESC,
    subject_id DESC
LIMIT 1000;