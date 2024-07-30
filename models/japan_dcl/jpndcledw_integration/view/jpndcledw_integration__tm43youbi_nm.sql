with derived_table1 AS
(
    SELECT 1 AS code,
        '日'::CHARACTER VARYING AS name
    UNION ALL
    SELECT 2 AS code,
        '月'::CHARACTER VARYING AS name
    UNION ALL
    SELECT 3 AS code,
        '火'::CHARACTER VARYING AS name
    UNION ALL
    SELECT 4 AS code,
        '水'::CHARACTER VARYING AS name
    UNION ALL
    SELECT 5 AS code,
        '木'::CHARACTER VARYING AS name
    UNION ALL
    SELECT 6 AS code,
        '金'::CHARACTER VARYING AS name
    UNION ALL
    SELECT 7 AS code,
        '土'::CHARACTER VARYING AS name
),
final AS
(
    SELECT derived_table1.code,
        COALESCE(derived_table1.name, ('その他'::CHARACTER VARYING)::CHARACTER VARYING(65535)) AS name,
        CASE 
            WHEN (
                    ((derived_table1.name)::TEXT = (NULL::CHARACTER VARYING)::TEXT)
                    OR derived_table1.name IS NULL
                    )
                THEN ('その他'::CHARACTER VARYING)::TEXT
            ELSE ((((derived_table1.code)::CHARACTER VARYING)::TEXT || (' : '::CHARACTER VARYING)::TEXT) || (derived_table1.name)::TEXT)
            END AS cname
    FROM derived_table1
)
select * from final
