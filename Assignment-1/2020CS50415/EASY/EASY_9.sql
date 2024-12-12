SELECT COUNT(*)
FROM admissions
WHERE hospital_expire_flag = 1
    AND hadm_id IN (
        SELECT hadm_id
        FROM labevents
        WHERE flag = 'abnormal'
    );