WITH final
AS (
    SELECT '81' AS code,
        'ポイント利用(交換以外)' AS name
    
    UNION ALL
    
    SELECT '82' AS code,
        '特典' AS name
    
    UNION ALL
    
    SELECT '83' AS code,
        'ポイント利用(交換)' AS name
    )
SELECT *
FROM final
