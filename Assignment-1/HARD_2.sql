-- WITH inner_tab AS (
--     SELECT diagnoses_icd.icd_code,
--         diagnoses_icd.icd_version,
--         admissions.subject_id,
--         admissions.hospital_expire_flag
--     FROM diagnoses_icd
--         INNER JOIN admissions ON admissions.subject_id = diagnoses_icd.subject_id
--         AND admissions.hadm_id = diagnoses_icd.hadm_id
-- ),
-- sum_tab AS (
--     SELECT inner_tab.icd_code,
--         inner_tab.icd_version,
--         SUM(
--             CASE
--                 WHEN inner_tab.hospital_expire_flag = 1 THEN 1
--                 ELSE 0
--             END
--         ) * 100.0 / COUNT(*) AS mortality_rate
--     FROM inner_tab
--     GROUP BY inner_tab.icd_code,
--         inner_tab.icd_version
--     ORDER BY mortality_rate DESC
-- ),
-- survived_tab AS (
--     SELECT DISTINCT i1.icd_code,
--         i1.icd_version,
--         i1.subject_id
--     FROM inner_tab AS i1
--         INNER JOIN (
--             SELECT DISTINCT icd_code,
--                 icd_version,
--                 subject_id
--             FROM inner_tab
--             WHERE hospital_expire_flag = 1
--         ) AS i2 ON i1.icd_code <> i2.icd_code
--         OR i1.icd_version <> i2.icd_version
--         OR i1.subject_id <> i2.subject_id
-- ),
-- avg_tab AS (
--     SELECT unique_patients.icd_code,
--         unique_patients.icd_version,
--         ROUND(
--             AVG(
--                 CASE
--                     WHEN unique_patients.hospital_expire_flag = 0 -- AND patients.subject_id NOT IN (
--                     --     SELECT subject_id
--                     --     FROM inner_tab AS inner_tab_2
--                     --     WHERE inner_tab_2.hospital_expire_flag = 1
--                     --         AND unique_patients.icd_code = inner_tab_2.icd_code
--                     --         AND unique_patients.icd_version = inner_tab_2.icd_version
--                     -- ) --     SELECT MAX(admissions.hospital_expire_flag)
--                     --     FROM admissions
--                     --     WHERE admissions.subject_id = unique_patients.subject_id
--                     -- )
--                     THEN patients.anchor_age
--                 END
--             ),
--             2
--         ) AS survived_avg_age
--     FROM patients
--         INNER JOIN survived_tab ON patients.subject_id = survived_tab.subject_id
--         -- (
--         --     SELECT DISTINCT icd_code,
--         --         icd_version,
--         --         subject_id -- ,hospital_expire_flag
--         --     FROM inner_tab AS inner_tab_1
--         --         INNER JOIN died_tab ON inner_tab_1.subject_id <> died_tab.subject_id
--         --         OR inner_tab_1.icd_code <> died_tab.icd_code
--         --         OR inner_tab_1.icd_version <> died_tab.icd_version -- WHERE hospital_expire_flag = 0
--         --         --     AND subject_id NOT IN (
--         --         --         SELECT subject_id
--         --         --         FROM inner_tab AS inner_tab_2
--         --         --         WHERE inner_tab_2.hospital_expire_flag = 1
--         --         --         AND inner_tab_1.icd_code = inner_tab_2.icd_code
--         --         --         AND inner_tab_1.icd_version = inner_tab_2.icd_version
--         --         --     )
--         -- ) AS unique_patients ON patients.subject_id = unique_patients.subject_id
--     GROUP BY unique_patients.icd_code,
--         unique_patients.icd_version
-- ),
-- final_tab AS (
--     SELECT d_icd_diagnoses.long_title,
--         avg_tab.survived_avg_age,
--         sum_tab.mortality_rate
--     FROM sum_tab
--         INNER JOIN avg_tab ON sum_tab.icd_code = avg_tab.icd_code
--         AND sum_tab.icd_version = avg_tab.icd_version
--         INNER JOIN d_icd_diagnoses ON avg_tab.icd_code = d_icd_diagnoses.icd_code
--         AND avg_tab.icd_version = d_icd_diagnoses.icd_version
--     ORDER BY mortality_rate DESC
--     LIMIT 245
-- )
-- SELECT long_title,
--     survived_avg_age
-- FROM final_tab
-- WHERE mortality_rate < 100.0
-- ORDER BY long_title ASC,
--     survived_avg_age DESC;
--
--
--
--
--
--
-- WITH final_tab AS (
--     SELECT d_icd_diagnoses.long_title,
--         ROUND(
--             AVG(
--                 CASE
--                     WHEN admissions.hospital_expire_flag = 0 THEN patients.anchor_age
--                 END
--             ),
--             2
--         ) AS survived_avg_age,
--         SUM(
--             CASE
--                 WHEN admissions.hospital_expire_flag = 1 THEN 1
--                 ELSE 0
--             END
--         ) * 100.0 / COUNT(*) AS mortality_rate
--     FROM d_icd_diagnoses
--         INNER JOIN diagnoses_icd ON diagnoses_icd.icd_code = d_icd_diagnoses.icd_code
--         AND diagnoses_icd.icd_version = d_icd_diagnoses.icd_version
--         INNER JOIN admissions ON admissions.subject_id = diagnoses_icd.subject_id
--         AND admissions.hadm_id = diagnoses_icd.hadm_id
--         INNER JOIN patients ON admissions.subject_id = patients.subject_id
--     GROUP BY d_icd_diagnoses.icd_code,
--         d_icd_diagnoses.icd_version
--     ORDER BY mortality_rate DESC
--     LIMIT 245
-- )
-- SELECT long_title,
--     survived_avg_age
-- FROM final_tab
-- WHERE mortality_rate < 100.0
-- ORDER BY long_title ASC,
--     survived_avg_age DESC