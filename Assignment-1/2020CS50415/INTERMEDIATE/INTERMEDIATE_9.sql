SELECT DISTINCT admissions.subject_id
FROM (
        SELECT first_diag.subject_id,
            admissions.dischtime
        FROM (
                SELECT heart_fail.subject_id,
                    MIN(admissions.admittime) AS admittime
                FROM (
                        SELECT DISTINCT subject_id,
                            hadm_id
                        FROM diagnoses_icd
                        WHERE STARTS_WITH(icd_code, 'I21')
                        GROUP BY subject_id,
                            hadm_id
                    ) AS heart_fail
                    INNER JOIN admissions ON heart_fail.hadm_id = admissions.hadm_id
                    AND heart_fail.subject_id = admissions.subject_id
                GROUP BY heart_fail.subject_id
            ) AS first_diag
            INNER JOIN admissions ON admissions.subject_id = first_diag.subject_id
            AND admissions.admittime = first_diag.admittime
    ) AS dis_time
    INNER JOIN admissions ON admissions.subject_id = dis_time.subject_id
    AND admissions.admittime > dis_time.dischtime
ORDER BY subject_id DESC
LIMIT 1000;
--
--
--
-- -----------------------------------------------------------
-- SELECT heart_fail.subject_id
-- FROM (
--         SELECT DISTINCT subject_id
--         FROM diagnoses_icd
--         WHERE STARTS_WITH(icd_code, 'I21')
--     ) AS heart_fail
--     INNER JOIN (
--         SELECT first_adm.subject_id
--         FROM (
--                 SELECT admissions.subject_id,
--                     admissions.dischtime
--                 FROM admissions
--                     INNER JOIN (
--                         SELECT subject_id,
--                             MIN(admittime) AS firsttime
--                         FROM admissions
--                         GROUP BY subject_id
--                     ) AS first_time ON admissions.subject_id = first_time.subject_id
--                     AND admissions.admittime = first_time.firsttime
--                 WHERE hadm_id IN (
--                         SELECT DISTINCT hadm_id
--                         FROM diagnoses_icd
--                         WHERE STARTS_WITH(icd_code, 'I21')
--                     )
--             ) AS first_adm
--             INNER JOIN (
--                 SELECT admissions.subject_id,
--                     admissions.admittime
--                 FROM admissions
--                     INNER JOIN (
--                         SELECT subject_id,
--                             admittime AS secondtime
--                         FROM (
--                                 SELECT subject_id,
--                                     admittime,
--                                     ROW_NUMBER() OVER (
--                                         PARTITION BY subject_id
--                                         ORDER BY admittime ASC
--                                     ) AS row_num
--                                 FROM admissions
--                             ) AS sub
--                         WHERE row_num = 2
--                     ) AS second_time ON admissions.subject_id = second_time.subject_id
--                     AND admissions.admittime = second_time.secondtime
--             ) AS second_adm ON first_adm.subject_id = second_adm.subject_id
--             AND first_adm.dischtime < second_adm.admittime
--     ) AS adm_info ON adm_info.subject_id = heart_fail.subject_id
-- ORDER BY subject_id DESC
-- LIMIT 1000 --
--
--
-- -----------------------------------------------------------
-- SELECT heart_fail.subject_id
-- FROM (
--         SELECT subject_id, hadm_id
--         FROM diagnoses_icd
--         WHERE STARTS_WITH(icd_code, 'I21')
--         GROUP BY subject_id, hadm_id
--     ) AS heart_fail
--     INNER JOIN (
--         SELECT first_adm.subject_id,
--             first_adm.hadm_id
--         FROM (
--                 SELECT admissions.subject_id,
--                     admissions.hadm_id,
--                     admissions.dischtime
--                 FROM admissions
--                     INNER JOIN (
--                         SELECT subject_id,
--                             MIN(admittime) AS firsttime
--                         FROM admissions
--                         GROUP BY subject_id
--                     ) AS first_time ON admissions.subject_id = first_time.subject_id
--                     AND admissions.admittime = first_time.firsttime
--             ) AS first_adm
--             INNER JOIN (
--                 SELECT admissions.subject_id,
--                     admissions.admittime
--                 FROM admissions
--                     INNER JOIN (
--                         SELECT subject_id,
--                             admittime AS secondtime
--                         FROM (
--                                 SELECT subject_id,
--                                     admittime,
--                                     ROW_NUMBER() OVER (
--                                         PARTITION BY subject_id
--                                         ORDER BY admittime ASC
--                                     ) AS row_num
--                                 FROM admissions
--                             ) AS sub
--                         WHERE row_num = 2
--                     ) AS second_time ON admissions.subject_id = second_time.subject_id
--                     AND admissions.admittime = second_time.secondtime
--             ) AS second_adm ON first_adm.subject_id = second_adm.subject_id
--             AND first_adm.dischtime < second_adm.admittime
--     ) AS adm_info ON adm_info.subject_id = heart_fail.subject_id
--     AND adm_info.hadm_id = heart_fail.hadm_id
-- ORDER BY subject_id DESC
-- LIMIT 1000