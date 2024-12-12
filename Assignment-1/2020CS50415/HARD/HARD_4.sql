WITH RECURSIVE graph AS(
    WITH forward AS (
        WITH early_adm AS (
            SELECT *
            FROM admissions
            ORDER BY admittime
            LIMIT 500
        )
        SELECT DISTINCT tab1.subject_id AS pat1,
            tab2.subject_id AS pat2
        FROM early_adm AS tab1
            INNER JOIN early_adm AS tab2 ON tab1.subject_id < tab2.subject_id
            AND tab1.admittime <= tab2.dischtime
            AND tab1.dischtime >= tab2.admittime
            INNER JOIN diagnoses_icd AS diag1 ON tab1.hadm_id = diag1.hadm_id
            AND tab1.subject_id = diag1.subject_id
            INNER JOIN diagnoses_icd AS diag2 ON tab2.hadm_id = diag2.hadm_id
            AND tab2.subject_id = diag2.subject_id
            AND diag1.icd_code = diag2.icd_code
            AND diag1.icd_version = diag2.icd_version
    )
    SELECT pat1,
        pat2
    FROM forward
    UNION
    SELECT pat2 AS pat1,
        pat1 AS pat2
    FROM forward
)
SELECT CASE
        WHEN EXISTS (
            SELECT *
            FROM graph
                INNER JOIN (
                    SELECT pat1,
                        pat2
                    FROM graph
                    WHERE pat1 IN (
                            SELECT pat2
                            FROM graph
                            WHERE pat1 = '18237734'
                        )
                        AND pat2 <> '18237734'
                ) AS two_down ON two_down.pat2 = graph.pat2
                AND two_down.pat1 <> graph.pat1
                AND graph.pat1 = '13401124'
        ) THEN 'True'
        ELSE 'False'
    END AS pathexists;
--
--
--
-- ,
-- graph_recursive AS (
--     SELECT pat1,
--         pat2,
--         1 AS depth
--     FROM graph
--     WHERE pat1 = '13401124'
--     UNION ALL
--     SELECT g.pat1,
--         g.pat2,
--         gr.depth + 1
--     FROM graph g
--         JOIN graph_recursive gr ON gr.pat2 = g.pat1 AND gr.pat1 <> g.pat2
--     WHERE gr.depth < 3
-- )
-- SELECT CASE
--         WHEN EXISTS (
--             SELECT 1
--             FROM graph_recursive
--             WHERE pat2 = '18237734'
--         ) THEN 'True'
--         ELSE 'False'
--     END;