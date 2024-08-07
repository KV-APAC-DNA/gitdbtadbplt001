WITH final AS 
(
    SELECT 
        derived_table1.code,
        COALESCE(derived_table1.name, 'その他') AS name,
        CASE 
            WHEN derived_table1.name IS NULL 
            THEN 'その他'
            ELSE derived_table1.code || ' : ' || derived_table1.name
        END AS cname
    FROM (
        SELECT 0 AS code, '未完了' AS name
    ) derived_table1
)

SELECT * FROM final