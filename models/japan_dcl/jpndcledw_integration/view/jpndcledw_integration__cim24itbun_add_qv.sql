WITH cte
AS (
    (
        (
            (
                (
                    (
                        (
                            (
                                SELECT '2' AS itbunshcode,
                                    '98501' AS itbuncode,
                                    'ポイント利用(交換以外)' AS itbunname
                                
                                UNION ALL
                                
                                SELECT '2' AS itbunshcode,
                                    '98502' AS itbuncode,
                                    'ポイント利用(交換)' AS itbunname
                                )
                            
                            UNION ALL
                            
                            SELECT '2' AS itbunshcode,
                                '98503' AS itbuncode,
                                '特典' AS itbunname
                            )
                        
                        UNION ALL
                        
                        SELECT '3' AS itbunshcode,
                            '98501' AS itbuncode,
                            'ポイント利用(交換以外)' AS itbunname
                        )
                    
                    UNION ALL
                    
                    SELECT '3' AS itbunshcode,
                        '98502' AS itbuncode,
                        'ポイント利用(交換)' AS itbunname
                    )
                
                UNION ALL
                
                SELECT '3' AS itbunshcode,
                    '98503' AS itbuncode,
                    '特典' AS itbunname
                )
            
            UNION ALL
            
            SELECT '5' AS itbunshcode,
                '71' AS itbuncode,
                'ポイント利用(交換以外)' AS itbunname
            )
        
        UNION ALL
        
        SELECT '5' AS itbunshcode,
            '72' AS itbuncode,
            'ポイント利用(交換)' AS itbunname
        )
    
    UNION ALL
    
    SELECT '5' AS itbunshcode, 
        '73' AS itbuncode,
        '特典' AS itbunname
    )
SELECT *
FROM cte
