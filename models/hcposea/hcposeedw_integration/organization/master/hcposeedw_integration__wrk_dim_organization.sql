WITH itg_USER
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_user') }}
    ),
itg_USERTERRITORY
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_userterritory') }}
    ),
itg_TERRITORY
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_territory') }}
    ),
t1
AS (
    SELECT DISTINCT ORG_HIER.TERRITORY_SOURCE_ID,
        COUNTRY_CODE,
        ORG_HIER.TERRITORY_SOURCE_ID AS MY_ORGANIZATION_CODE,
        ORG_HIER.TERRITORY_NAME AS MY_ORGANIZATION_NAME,
        ORG_HIER.LEVEL_1_TERRITORY_CODE AS ORGANIZATION_L1_CODE,
        ORG_HIER.LEVEL_1_TERRITORY_NAME AS ORGANIZATION_L1_NAME,
        ORG_HIER.LEVEL_2_TERRITORY_CODE AS ORGANIZATION_L2_CODE,
        ORG_HIER.LEVEL_2_TERRITORY_NAME AS ORGANIZATION_L2_NAME,
        ORG_HIER.LEVEL_3_TERRITORY_CODE AS ORGANIZATION_L3_CODE,
        ORG_HIER.LEVEL_3_TERRITORY_NAME AS ORGANIZATION_L3_NAME,
        ORG_HIER.LEVEL_4_TERRITORY_CODE AS ORGANIZATION_L4_CODE,
        ORG_HIER.LEVEL_4_TERRITORY_NAME AS ORGANIZATION_L4_NAME,
        ORG_HIER.LEVEL_5_TERRITORY_CODE AS ORGANIZATION_L5_CODE,
        ORG_HIER.LEVEL_5_TERRITORY_NAME AS ORGANIZATION_L5_NAME,
        ORG_HIER.LEVEL_6_TERRITORY_CODE AS ORGANIZATION_L6_CODE,
        ORG_HIER.LEVEL_6_TERRITORY_NAME AS ORGANIZATION_L6_NAME,
        ORG_HIER.LEVEL_7_TERRITORY_CODE AS ORGANIZATION_L7_CODE,
        ORG_HIER.LEVEL_7_TERRITORY_NAME AS ORGANIZATION_L7_NAME,
        ORG_HIER.LEVEL_8_TERRITORY_CODE AS ORGANIZATION_L8_CODE,
        ORG_HIER.LEVEL_8_TERRITORY_NAME AS ORGANIZATION_L8_NAME,
        ORG_HIER.LEVEL_9_TERRITORY_CODE AS ORGANIZATION_L9_CODE,
        ORG_HIER.LEVEL_9_TERRITORY_NAME AS ORGANIZATION_L9_NAME,
        current_timestamp() AS INSERTED_DATE,
        current_timestamp() AS UPDATED_DATE
    FROM
        ---- ORG HIERARCHY FOR TH
        (
        -- LEAF NODE
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'TH'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    ),
                LEVEL_1 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_2
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_1.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_1.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3,
                LEVEL_2,
                LEVEL_1
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND LEVEL_2.PARENT_TERRITORY_SOURCE_ID = LEVEL_1.TERRITORY_SOURCE_ID
            )
        
        UNION ALL
        
        --- LEVEL 3
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'TH'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3,
                LEVEL_2
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_2.TERRITORY_NAME) = 'TH'
            )
        
        UNION ALL
        
        --- LEVEL 2 
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'TH'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_3.TERRITORY_NAME) = 'TH'
            )
        ) ORG_HIER
    ),
t2
AS (
    SELECT DISTINCT ORG_HIER.TERRITORY_SOURCE_ID,
        COUNTRY_CODE,
        ORG_HIER.TERRITORY_SOURCE_ID AS MY_ORGANIZATION_CODE,
        ORG_HIER.TERRITORY_NAME AS MY_ORGANIZATION_NAME,
        ORG_HIER.LEVEL_1_TERRITORY_CODE AS ORGANIZATION_L1_CODE,
        ORG_HIER.LEVEL_1_TERRITORY_NAME AS ORGANIZATION_L1_NAME,
        ORG_HIER.LEVEL_2_TERRITORY_CODE AS ORGANIZATION_L2_CODE,
        ORG_HIER.LEVEL_2_TERRITORY_NAME AS ORGANIZATION_L2_NAME,
        ORG_HIER.LEVEL_3_TERRITORY_CODE AS ORGANIZATION_L3_CODE,
        ORG_HIER.LEVEL_3_TERRITORY_NAME AS ORGANIZATION_L3_NAME,
        ORG_HIER.LEVEL_4_TERRITORY_CODE AS ORGANIZATION_L4_CODE,
        ORG_HIER.LEVEL_4_TERRITORY_NAME AS ORGANIZATION_L4_NAME,
        ORG_HIER.LEVEL_5_TERRITORY_CODE AS ORGANIZATION_L5_CODE,
        ORG_HIER.LEVEL_5_TERRITORY_NAME AS ORGANIZATION_L5_NAME,
        ORG_HIER.LEVEL_6_TERRITORY_CODE AS ORGANIZATION_L6_CODE,
        ORG_HIER.LEVEL_6_TERRITORY_NAME AS ORGANIZATION_L6_NAME,
        ORG_HIER.LEVEL_7_TERRITORY_CODE AS ORGANIZATION_L7_CODE,
        ORG_HIER.LEVEL_7_TERRITORY_NAME AS ORGANIZATION_L7_NAME,
        ORG_HIER.LEVEL_8_TERRITORY_CODE AS ORGANIZATION_L8_CODE,
        ORG_HIER.LEVEL_8_TERRITORY_NAME AS ORGANIZATION_L8_NAME,
        ORG_HIER.LEVEL_9_TERRITORY_CODE AS ORGANIZATION_L9_CODE,
        ORG_HIER.LEVEL_9_TERRITORY_NAME AS ORGANIZATION_L9_NAME,
        current_timestamp(),
        current_timestamp()
    FROM
        ---- ORG HIERARCHY FOR VN
        (
        -- LEAF NODE
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'VN'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    ),
                LEVEL_1 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_2
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_1.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_1.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4,
                LEVEL_3,
                LEVEL_2,
                LEVEL_1
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND LEVEL_2.PARENT_TERRITORY_SOURCE_ID = LEVEL_1.TERRITORY_SOURCE_ID
            )
        
        UNION ALL
        
        --LEVEL 4
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'VN'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_4_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4,
                LEVEL_3,
                LEVEL_2
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_2.TERRITORY_NAME) = 'VN'
            )
        
        UNION ALL
        
        --- LEVEL 3
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'VN'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4,
                LEVEL_3
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_3.TERRITORY_NAME) = 'VN'
            )
        
        UNION ALL
        
        --- LEVEL 2 
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'VN'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_4.TERRITORY_NAME) = 'VN'
            )
        ) ORG_HIER
    ),
t3
AS (
    SELECT DISTINCT ORG_HIER.TERRITORY_SOURCE_ID,
        COUNTRY_CODE,
        ORG_HIER.TERRITORY_SOURCE_ID AS MY_ORGANIZATION_CODE,
        ORG_HIER.TERRITORY_NAME AS MY_ORGANIZATION_NAME,
        ORG_HIER.LEVEL_1_TERRITORY_CODE AS ORGANIZATION_L1_CODE,
        ORG_HIER.LEVEL_1_TERRITORY_NAME AS ORGANIZATION_L1_NAME,
        ORG_HIER.LEVEL_2_TERRITORY_CODE AS ORGANIZATION_L2_CODE,
        ORG_HIER.LEVEL_2_TERRITORY_NAME AS ORGANIZATION_L2_NAME,
        ORG_HIER.LEVEL_3_TERRITORY_CODE AS ORGANIZATION_L3_CODE,
        ORG_HIER.LEVEL_3_TERRITORY_NAME AS ORGANIZATION_L3_NAME,
        ORG_HIER.LEVEL_4_TERRITORY_CODE AS ORGANIZATION_L4_CODE,
        ORG_HIER.LEVEL_4_TERRITORY_NAME AS ORGANIZATION_L4_NAME,
        ORG_HIER.LEVEL_5_TERRITORY_CODE AS ORGANIZATION_L5_CODE,
        ORG_HIER.LEVEL_5_TERRITORY_NAME AS ORGANIZATION_L5_NAME,
        ORG_HIER.LEVEL_6_TERRITORY_CODE AS ORGANIZATION_L6_CODE,
        ORG_HIER.LEVEL_6_TERRITORY_NAME AS ORGANIZATION_L6_NAME,
        ORG_HIER.LEVEL_7_TERRITORY_CODE AS ORGANIZATION_L7_CODE,
        ORG_HIER.LEVEL_7_TERRITORY_NAME AS ORGANIZATION_L7_NAME,
        ORG_HIER.LEVEL_8_TERRITORY_CODE AS ORGANIZATION_L8_CODE,
        ORG_HIER.LEVEL_8_TERRITORY_NAME AS ORGANIZATION_L8_NAME,
        ORG_HIER.LEVEL_9_TERRITORY_CODE AS ORGANIZATION_L9_CODE,
        ORG_HIER.LEVEL_9_TERRITORY_NAME AS ORGANIZATION_L9_NAME,
        current_timestamp(),
        current_timestamp()
    FROM
        ---- ORG HIERARCHY FOR PH 
        (
        -- LEAF NODE
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'PH'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    ),
                LEVEL_1 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_2
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_1.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_1.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3,
                LEVEL_2,
                LEVEL_1
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND LEVEL_2.PARENT_TERRITORY_SOURCE_ID = LEVEL_1.TERRITORY_SOURCE_ID
            )
        
        UNION ALL
        
        --- LEVEL 3
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'PH'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3,
                LEVEL_2
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_2.TERRITORY_NAME) = 'PH'
            )
        
        UNION ALL
        
        --- LEVEL 2 
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'PH'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_3.TERRITORY_NAME) = 'PH'
            )
        ) ORG_HIER
    ),
t4
AS (
    SELECT DISTINCT ORG_HIER.TERRITORY_SOURCE_ID,
        COUNTRY_CODE,
        ORG_HIER.TERRITORY_SOURCE_ID AS MY_ORGANIZATION_CODE,
        ORG_HIER.TERRITORY_NAME AS MY_ORGANIZATION_NAME,
        ORG_HIER.LEVEL_1_TERRITORY_CODE AS ORGANIZATION_L1_CODE,
        ORG_HIER.LEVEL_1_TERRITORY_NAME AS ORGANIZATION_L1_NAME,
        ORG_HIER.LEVEL_2_TERRITORY_CODE AS ORGANIZATION_L2_CODE,
        ORG_HIER.LEVEL_2_TERRITORY_NAME AS ORGANIZATION_L2_NAME,
        ORG_HIER.LEVEL_3_TERRITORY_CODE AS ORGANIZATION_L3_CODE,
        ORG_HIER.LEVEL_3_TERRITORY_NAME AS ORGANIZATION_L3_NAME,
        ORG_HIER.LEVEL_4_TERRITORY_CODE AS ORGANIZATION_L4_CODE,
        ORG_HIER.LEVEL_4_TERRITORY_NAME AS ORGANIZATION_L4_NAME,
        ORG_HIER.LEVEL_5_TERRITORY_CODE AS ORGANIZATION_L5_CODE,
        ORG_HIER.LEVEL_5_TERRITORY_NAME AS ORGANIZATION_L5_NAME,
        ORG_HIER.LEVEL_6_TERRITORY_CODE AS ORGANIZATION_L6_CODE,
        ORG_HIER.LEVEL_6_TERRITORY_NAME AS ORGANIZATION_L6_NAME,
        ORG_HIER.LEVEL_7_TERRITORY_CODE AS ORGANIZATION_L7_CODE,
        ORG_HIER.LEVEL_7_TERRITORY_NAME AS ORGANIZATION_L7_NAME,
        ORG_HIER.LEVEL_8_TERRITORY_CODE AS ORGANIZATION_L8_CODE,
        ORG_HIER.LEVEL_8_TERRITORY_NAME AS ORGANIZATION_L8_NAME,
        ORG_HIER.LEVEL_9_TERRITORY_CODE AS ORGANIZATION_L9_CODE,
        ORG_HIER.LEVEL_9_TERRITORY_NAME AS ORGANIZATION_L9_NAME,
        current_timestamp(),
        current_timestamp()
    FROM
        ---- ORG HIERARCHY FOR SG
        (
        -- LEAF NODE
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'SG'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    ),
                LEVEL_1 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_2
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_1.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_1.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3,
                LEVEL_2,
                LEVEL_1
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND LEVEL_2.PARENT_TERRITORY_SOURCE_ID = LEVEL_1.TERRITORY_SOURCE_ID
            )
        
        UNION ALL
        
        --- LEVEL 3
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'SG'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3,
                LEVEL_2
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_2.TERRITORY_NAME) = 'SG'
            )
        
        UNION ALL
        
        --- LEVEL 2 
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'SG'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                '' AS LEVEL_5_TERRITORY_CODE,
                '' AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_3
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_3.TERRITORY_NAME) = 'SG'
            )
        ) ORG_HIER
    ),
t5
AS (
    SELECT DISTINCT ORG_HIER.TERRITORY_SOURCE_ID,
        COUNTRY_CODE,
        ORG_HIER.TERRITORY_SOURCE_ID AS MY_ORGANIZATION_CODE,
        ORG_HIER.TERRITORY_NAME AS MY_ORGANIZATION_NAME,
        ORG_HIER.LEVEL_1_TERRITORY_CODE AS ORGANIZATION_L1_CODE,
        ORG_HIER.LEVEL_1_TERRITORY_NAME AS ORGANIZATION_L1_NAME,
        ORG_HIER.LEVEL_2_TERRITORY_CODE AS ORGANIZATION_L2_CODE,
        ORG_HIER.LEVEL_2_TERRITORY_NAME AS ORGANIZATION_L2_NAME,
        ORG_HIER.LEVEL_3_TERRITORY_CODE AS ORGANIZATION_L3_CODE,
        ORG_HIER.LEVEL_3_TERRITORY_NAME AS ORGANIZATION_L3_NAME,
        ORG_HIER.LEVEL_4_TERRITORY_CODE AS ORGANIZATION_L4_CODE,
        ORG_HIER.LEVEL_4_TERRITORY_NAME AS ORGANIZATION_L4_NAME,
        ORG_HIER.LEVEL_5_TERRITORY_CODE AS ORGANIZATION_L5_CODE,
        ORG_HIER.LEVEL_5_TERRITORY_NAME AS ORGANIZATION_L5_NAME,
        ORG_HIER.LEVEL_6_TERRITORY_CODE AS ORGANIZATION_L6_CODE,
        ORG_HIER.LEVEL_6_TERRITORY_NAME AS ORGANIZATION_L6_NAME,
        ORG_HIER.LEVEL_7_TERRITORY_CODE AS ORGANIZATION_L7_CODE,
        ORG_HIER.LEVEL_7_TERRITORY_NAME AS ORGANIZATION_L7_NAME,
        ORG_HIER.LEVEL_8_TERRITORY_CODE AS ORGANIZATION_L8_CODE,
        ORG_HIER.LEVEL_8_TERRITORY_NAME AS ORGANIZATION_L8_NAME,
        ORG_HIER.LEVEL_9_TERRITORY_CODE AS ORGANIZATION_L9_CODE,
        ORG_HIER.LEVEL_9_TERRITORY_NAME AS ORGANIZATION_L9_NAME,
        current_timestamp(),
        current_timestamp()
    FROM
        ---- ORG HIERARCHY FOR ID
        (
        -- LEAF NODE
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'ID'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    ),
                LEVEL_1 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_2
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_1.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_1.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4,
                LEVEL_3,
                LEVEL_2,
                LEVEL_1
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND LEVEL_2.PARENT_TERRITORY_SOURCE_ID = LEVEL_1.TERRITORY_SOURCE_ID
            )
        
        UNION ALL
        
        --LEVEL 4
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'ID'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_4_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4,
                LEVEL_3,
                LEVEL_2
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_2.TERRITORY_NAME) = 'ID'
            )
        
        UNION ALL
        
        --- LEVEL 3
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'ID'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4,
                LEVEL_3
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_3.TERRITORY_NAME) = 'ID'
            )
        
        UNION ALL
        
        --- LEVEL 2 
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'ID'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                '' AS LEVEL_6_TERRITORY_CODE,
                '' AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_4
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_4.TERRITORY_NAME) = 'ID'
            )
        ) ORG_HIER
    ),
t6
AS (
    SELECT DISTINCT ORG_HIER.TERRITORY_SOURCE_ID,
        COUNTRY_CODE,
        ORG_HIER.TERRITORY_SOURCE_ID AS MY_ORGANIZATION_CODE,
        ORG_HIER.TERRITORY_NAME AS MY_ORGANIZATION_NAME,
        ORG_HIER.LEVEL_1_TERRITORY_CODE AS ORGANIZATION_L1_CODE,
        ORG_HIER.LEVEL_1_TERRITORY_NAME AS ORGANIZATION_L1_NAME,
        ORG_HIER.LEVEL_2_TERRITORY_CODE AS ORGANIZATION_L2_CODE,
        ORG_HIER.LEVEL_2_TERRITORY_NAME AS ORGANIZATION_L2_NAME,
        ORG_HIER.LEVEL_3_TERRITORY_CODE AS ORGANIZATION_L3_CODE,
        ORG_HIER.LEVEL_3_TERRITORY_NAME AS ORGANIZATION_L3_NAME,
        ORG_HIER.LEVEL_4_TERRITORY_CODE AS ORGANIZATION_L4_CODE,
        ORG_HIER.LEVEL_4_TERRITORY_NAME AS ORGANIZATION_L4_NAME,
        ORG_HIER.LEVEL_5_TERRITORY_CODE AS ORGANIZATION_L5_CODE,
        ORG_HIER.LEVEL_5_TERRITORY_NAME AS ORGANIZATION_L5_NAME,
        ORG_HIER.LEVEL_6_TERRITORY_CODE AS ORGANIZATION_L6_CODE,
        ORG_HIER.LEVEL_6_TERRITORY_NAME AS ORGANIZATION_L6_NAME,
        ORG_HIER.LEVEL_7_TERRITORY_CODE AS ORGANIZATION_L7_CODE,
        ORG_HIER.LEVEL_7_TERRITORY_NAME AS ORGANIZATION_L7_NAME,
        ORG_HIER.LEVEL_8_TERRITORY_CODE AS ORGANIZATION_L8_CODE,
        ORG_HIER.LEVEL_8_TERRITORY_NAME AS ORGANIZATION_L8_NAME,
        ORG_HIER.LEVEL_9_TERRITORY_CODE AS ORGANIZATION_L9_CODE,
        ORG_HIER.LEVEL_9_TERRITORY_NAME AS ORGANIZATION_L9_NAME,
        current_timestamp(),
        current_timestamp()
    FROM
        ---- ORG HIERARCHY FOR MY
        (
        -- LEAF NODE
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'MY'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_5 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_5
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    ),
                LEVEL_1 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_2
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_6_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_6_TERRITORY_NAME,
                LEVEL_5.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEVEL_5.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_1.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_1.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_5,
                LEVEL_4,
                LEVEL_3,
                LEVEL_2,
                LEVEL_1
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_5.TERRITORY_SOURCE_ID
                AND LEVEL_5.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND LEVEL_2.PARENT_TERRITORY_SOURCE_ID = LEVEL_1.TERRITORY_SOURCE_ID
            )
        
        UNION ALL
        
        -- LEVEL 5
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'MY'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_5 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_5
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    ),
                LEVEL_2 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_3
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_6_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEVEL_5.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEVEL_5.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_2.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_2.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_5,
                LEVEL_4,
                LEVEL_3,
                LEVEL_2
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_5.TERRITORY_SOURCE_ID
                AND LEVEL_5.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND LEVEL_3.PARENT_TERRITORY_SOURCE_ID = LEVEL_2.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_2.TERRITORY_NAME) = 'MY'
            )
        
        UNION ALL
        
        --LEVEL 4
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'MY'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_5 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_5
                            )
                    ),
                LEVEL_3 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_4
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_6_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEVEL_5.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEVEL_5.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_3.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_3.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_5,
                LEVEL_4,
                LEVEL_3
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_5.TERRITORY_SOURCE_ID
                AND LEVEL_5.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND LEVEL_4.PARENT_TERRITORY_SOURCE_ID = LEVEL_3.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_3.TERRITORY_NAME) = 'MY'
            )
        
        UNION ALL
        
        --- LEVEL 3
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'MY'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_5 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    ),
                LEVEL_4 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM LEVEL_5
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_6_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEVEL_5.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEVEL_5.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_4.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_4.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_5,
                LEVEL_4
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_5.TERRITORY_SOURCE_ID
                AND LEVEL_5.PARENT_TERRITORY_SOURCE_ID = LEVEL_4.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_4.TERRITORY_NAME) = 'MY'
            )
        
        UNION ALL
        
        --- LEVEL 2 
        (
            WITH BASE_USER AS (
                    SELECT USER_NAME,
                        EMPLOYEE_NAME,
                        COMPANY_NAME,
                        DEPARTMENT,
                        TERRITORY_NAME,
                        T.TERRITORY_SOURCE_ID,
                        PARENT_TERRITORY_SOURCE_ID,
                        U.COUNTRY_CODE
                    FROM itg_USER U,
                        itg_USERTERRITORY UT,
                        itg_TERRITORY T
                    WHERE EMPLOYEE_SOURCE_ID = USER_TERRITORY_USER_SOURCE_ID
                        AND UT.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
                        AND UT.IS_ACTIVE = '1'
                        AND U.COUNTRY_CODE = 'MY'
                    ),
                ITG_HCP360_OSEA_TERRITORY AS (
                    SELECT *
                    FROM itg_TERRITORY T
                    WHERE territory_name NOT IN ('ASEANHK', 'SIMP')
                    ),
                LEAF_NODE AS (
                    SELECT *
                    FROM BASE_USER
                    ),
                LEVEL_5 AS (
                    SELECT *
                    FROM ITG_HCP360_OSEA_TERRITORY
                    WHERE (TERRITORY_SOURCE_ID) IN (
                            SELECT PARENT_TERRITORY_SOURCE_ID
                            FROM BASE_USER
                            )
                    )
            SELECT DISTINCT BASE_USER.USER_NAME,
                BASE_USER.EMPLOYEE_NAME,
                BASE_USER.COMPANY_NAME,
                BASE_USER.DEPARTMENT,
                BASE_USER.TERRITORY_NAME,
                BASE_USER.TERRITORY_SOURCE_ID,
                BASE_USER.COUNTRY_CODE,
                '' AS LEVEL_9_TERRITORY_CODE,
                '' AS LEVEL_9_TERRITORY_NAME,
                '' AS LEVEL_8_TERRITORY_CODE,
                '' AS LEVEL_8_TERRITORY_NAME,
                '' AS LEVEL_7_TERRITORY_CODE,
                '' AS LEVEL_7_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_6_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_6_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID AS LEVEL_5_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME AS LEVEL_5_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_4_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_4_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_3_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_3_TERRITORY_NAME,
                LEAF_NODE.TERRITORY_SOURCE_ID LEVEL_2_TERRITORY_CODE,
                LEAF_NODE.TERRITORY_NAME LEVEL_2_TERRITORY_NAME,
                LEVEL_5.TERRITORY_SOURCE_ID LEVEL_1_TERRITORY_CODE,
                LEVEL_5.TERRITORY_NAME LEVEL_1_TERRITORY_NAME
            FROM BASE_USER,
                LEAF_NODE,
                LEVEL_5
            WHERE BASE_USER.TERRITORY_SOURCE_ID = LEAF_NODE.TERRITORY_SOURCE_ID
                AND LEAF_NODE.PARENT_TERRITORY_SOURCE_ID = LEVEL_5.TERRITORY_SOURCE_ID
                AND UPPER(LEVEL_5.TERRITORY_NAME) = 'MY'
            )
        ) ORG_HIER

    ),
JOINED
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    
    UNION ALL
    
    SELECT *
    FROM T3
    
    UNION ALL
    
    SELECT *
    FROM T4
    
    UNION ALL
    
    SELECT *
    FROM T5
    
    UNION ALL
    
    SELECT *
    FROM T6
    ),
FINAL AS
(
    SELECT TERRITORY_SOURCE_ID::VARCHAR(18) AS TERRITORY_SOURCE_ID,
        COUNTRY_CODE::VARCHAR(8) AS COUNTRY_CODE,
        MY_ORGANIZATION_CODE::VARCHAR(18) AS MY_ORGANIZATION_CODE,
        MY_ORGANIZATION_NAME::VARCHAR(80) AS MY_ORGANIZATION_NAME,
        ORGANIZATION_L1_CODE::VARCHAR(18) AS ORGANIZATION_L1_CODE,
        ORGANIZATION_L1_NAME::VARCHAR(80) AS ORGANIZATION_L1_NAME,
        ORGANIZATION_L2_CODE::VARCHAR(18) AS ORGANIZATION_L2_CODE,
        ORGANIZATION_L2_NAME::VARCHAR(80) AS ORGANIZATION_L2_NAME,
        ORGANIZATION_L3_CODE::VARCHAR(18) AS ORGANIZATION_L3_CODE,
        ORGANIZATION_L3_NAME::VARCHAR(80) AS ORGANIZATION_L3_NAME,
        ORGANIZATION_L4_CODE::VARCHAR(18) AS ORGANIZATION_L4_CODE,
        ORGANIZATION_L4_NAME::VARCHAR(80) AS ORGANIZATION_L4_NAME,
        ORGANIZATION_L5_CODE::VARCHAR(18) AS ORGANIZATION_L5_CODE,
        ORGANIZATION_L5_NAME::VARCHAR(80) AS ORGANIZATION_L5_NAME,
        ORGANIZATION_L6_CODE::VARCHAR(18) AS ORGANIZATION_L6_CODE,
        ORGANIZATION_L6_NAME::VARCHAR(80) AS ORGANIZATION_L6_NAME,
        ORGANIZATION_L7_CODE::VARCHAR(18) AS ORGANIZATION_L7_CODE,
        ORGANIZATION_L7_NAME::VARCHAR(80) AS ORGANIZATION_L7_NAME,
        ORGANIZATION_L8_CODE::VARCHAR(18) AS ORGANIZATION_L8_CODE,
        ORGANIZATION_L8_NAME::VARCHAR(80) AS ORGANIZATION_L8_NAME,
        ORGANIZATION_L9_CODE::VARCHAR(18) AS ORGANIZATION_L9_CODE,
        ORGANIZATION_L9_NAME::VARCHAR(80) AS ORGANIZATION_L9_NAME,
        INSERTED_DATE::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        UPDATED_DATE::TIMESTAMP_NTZ(9) AS UPDATED_DATE

    FROM JOINED
)
select * from final