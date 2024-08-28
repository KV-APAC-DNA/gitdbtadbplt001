with itg_territory as (
    select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_TERRITORY
),

itg_userterritory as (
    select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_USERTERRITORY
),

itg_user as (
    select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_USER
),

itg_profile as (
    select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_PROFILE
),

itg_lookup_eng_data as (
    select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_LOOKUP_ENG_DATA
),

wrk_dim_organization as (
    select * from {{ ref('hcposeedw_integration__wrk_dim_organization') }}
),

IQ AS (
        SELECT DISTINCT IU.EMPLOYEE_KEY,
            IU.COUNTRY_CODE,
            IU.EMPLOYEE_SOURCE_ID,
            IU.LAST_MODIFIED_DATE,
            IU.LAST_MODIFIED_BY_ID,
            IU.EMPLOYEE_NAME,
            IU.WWID,
            IU.MOBILE_PHONE,
            IU.EMAIL,
            IU.USER_NAME,
            IU.NICKNAME,
            IU.LOCAL_EMPLOYEE_NUMBER,
            IP.PROFILE_NAME,
            IU.EMPLOYEE_PROFILE_ID,
            PROF_NAME.TARGET_VALUE,
            IU.COMPANY_NAME,
            IU.DIVISION,
            IU.DEPARTMENT,
            IU.COUNTRY,
            IU.ADDRESS,
            IU.ALIAS,
            IU.TIMEZONESIDKEY,
            IU.USER_ROLE_SOURCE_ID,
            CAST(IU.RECEIVES_INFO_EMAILS AS INTEGER) AS RECEIVES_INFO_EMAILS,
            IU.FEDERATION_IDENTIFIER,
            IU.LAST_IPAD_SYNC,
            IU.USER_LICENSE,
            IU.title,
            IU.phone,
            IU.last_login_date,
            IU.region,
            IU.profile_group_ap,
            IU.manager_name,
            IU.manager_wwid,
            CAST(IU.is_active AS INTEGER) AS active_flag,
            ORG_HIER.MY_ORGANIZATION_CODE,
            ORG_HIER.MY_ORGANIZATION_NAME,
            ORG_HIER.ORGANIZATION_L1_CODE,
            ORG_HIER.ORGANIZATION_L1_NAME,
            ORG_HIER.ORGANIZATION_L2_CODE,
            ORG_HIER.ORGANIZATION_L2_NAME,
            ORG_HIER.ORGANIZATION_L3_CODE,
            ORG_HIER.ORGANIZATION_L3_NAME,
            ORG_HIER.ORGANIZATION_L4_CODE,
            ORG_HIER.ORGANIZATION_L4_NAME,
            ORG_HIER.ORGANIZATION_L5_CODE,
            ORG_HIER.ORGANIZATION_L5_NAME,
            ORG_HIER.ORGANIZATION_L6_CODE,
            ORG_HIER.ORGANIZATION_L6_NAME,
            ORG_HIER.ORGANIZATION_L7_CODE,
            ORG_HIER.ORGANIZATION_L7_NAME,
            ORG_HIER.ORGANIZATION_L8_CODE,
            ORG_HIER.ORGANIZATION_L8_NAME,
            ORG_HIER.ORGANIZATION_L9_CODE,
            ORG_HIER.ORGANIZATION_L9_NAME,
            ORG_HIER.ORGANIZATION_L1_CODE AS COMMON_ORGANIZATION_L1_CODE,
            ORG_HIER.ORGANIZATION_L1_NAME AS COMMON_ORGANIZATION_L1_NAME,
            ORG_HIER.ORGANIZATION_L2_CODE AS COMMON_ORGANIZATION_L2_CODE,
            ORG_HIER.ORGANIZATION_L2_NAME AS COMMON_ORGANIZATION_L2_NAME,
            ORG_HIER.ORGANIZATION_L3_CODE AS COMMON_ORGANIZATION_L3_CODE,
            ORG_HIER.ORGANIZATION_L3_NAME AS COMMON_ORGANIZATION_L3_NAME,
            IU.MSL_PRIMARY_RESPONSIBLE_TA,
            IU.MSL_SECONDARY_RESPONSIBLE_TA,
            IU.city,
            IU.first_name,
            IU.language_local_key,
            IU.last_name,
            IU.manager_source_id,
            IU.postal_code,
            IU.STATE,
            IU.user_type,
            IU.veeva_user_type,
            IU.veeva_country_code,
            IU.shc_user_franchise
        FROM itg_territory IT,
            itg_userterritory IUT,
            itg_user IU,
            itg_profile IP,
            wrk_dim_organization ORG_HIER,
            (
                SELECT COUNTRY_CODE,
                    KEY_VALUE,
                    TARGET_VALUE
                FROM itg_lookup_eng_data
                WHERE trim(UPPER(TABLE_NAME)) = 'DIM_PROFILE'
                    AND trim(UPPER(COLUMN_NAME)) = 'PROFILE_NAME'
                    AND trim(UPPER(TARGET_COLUMN_NAME)) = 'FUNCTION_NAME'
                ) PROF_NAME
        WHERE trim(IU.EMPLOYEE_SOURCE_ID) = trim(IUT.USER_TERRITORY_USER_SOURCE_ID(+))
            --and iu.country_code = iut.country_code(+)  -- country code is null for IUT
            AND trim(IUT.TERRITORY_SOURCE_ID) = trim(IT.TERRITORY_SOURCE_ID(+))
            --and iut.country_code = it.country_code (+) --- Country code is null for IT
            AND trim(IU.EMPLOYEE_PROFILE_ID) = trim(IP.PROFILE_SOURCE_ID(+))
            AND trim(IP.PROFILE_NAME) = trim(PROF_NAME.KEY_VALUE(+))
            AND trim(IP.COUNTRY_CODE) = trim(PROF_NAME.COUNTRY_CODE(+))
            AND trim(IT.TERRITORY_SOURCE_ID) = trim(ORG_HIER.TERRITORY_SOURCE_ID(+))
        ),

wrk_dim_employee_temp
AS (
    SELECT IQ.*,
    CASE 
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                AND IQ.ORGANIZATION_L4_CODE <> ''
                AND IQ.ORGANIZATION_L5_CODE <> ''
                AND IQ.ORGANIZATION_L6_CODE <> ''
                AND IQ.ORGANIZATION_L7_CODE <> ''
                AND IQ.ORGANIZATION_L8_CODE <> ''
                AND IQ.ORGANIZATION_L9_CODE <> ''
                )
            THEN 1
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                AND IQ.ORGANIZATION_L4_CODE <> ''
                AND IQ.ORGANIZATION_L5_CODE <> ''
                AND IQ.ORGANIZATION_L6_CODE <> ''
                AND IQ.ORGANIZATION_L7_CODE <> ''
                AND IQ.ORGANIZATION_L8_CODE <> ''
                )
            THEN 2
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                AND IQ.ORGANIZATION_L4_CODE <> ''
                AND IQ.ORGANIZATION_L5_CODE <> ''
                AND IQ.ORGANIZATION_L6_CODE <> ''
                AND IQ.ORGANIZATION_L7_CODE <> ''
                )
            THEN 3
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                AND IQ.ORGANIZATION_L4_CODE <> ''
                AND IQ.ORGANIZATION_L5_CODE <> ''
                AND IQ.ORGANIZATION_L6_CODE <> ''
                )
            THEN 4
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                AND IQ.ORGANIZATION_L4_CODE <> ''
                AND IQ.ORGANIZATION_L5_CODE <> ''
                )
            THEN 5
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                AND IQ.ORGANIZATION_L4_CODE <> ''
                )
            THEN 6
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                AND IQ.ORGANIZATION_L3_CODE <> ''
                )
            THEN 7
        WHEN (
                IQ.ORGANIZATION_L1_CODE <> ''
                AND IQ.ORGANIZATION_L2_CODE <> ''
                )
            THEN 8
        WHEN (IQ.ORGANIZATION_L1_CODE <> '')
            THEN 9
        ELSE 0
        END RNK
FROM IQ
    ),

T1
AS (
    SELECT IQ.EMPLOYEE_KEY,
        IQ.COUNTRY_CODE,
        NVL(IQ.EMPLOYEE_SOURCE_ID, 'Not Applicable') AS EMPLOYEE_SOURCE_ID,
        IQ.LAST_MODIFIED_DATE,
        NVL(IQ.LAST_MODIFIED_BY_ID, 'Not Applicable') AS LAST_MODIFIED_BY_ID,
        IQ.EMPLOYEE_NAME,
        IQ.WWID,
        IQ.MOBILE_PHONE,
        IQ.EMAIL,
        IQ.USER_NAME,
        IQ.NICKNAME,
        IQ.LOCAL_EMPLOYEE_NUMBER,
        NVL(IQ.PROFILE_ID, 'Not Applicable') AS PROFILE_ID,
        IQ.PROFILE_NAME,
        IQ.TARGET_VALUE,
        NVL(IQ.EMPLOYEE_PROFILE_ID, 'Not Applicable') AS EMPLOYEE_PROFILE_ID,
        IQ.COMPANY_NAME,
        IQ.DIVISION,
        IQ.DEPARTMENT,
        IQ.COUNTRY,
        IQ.ADDRESS,
        IQ.ALIAS,
        IQ.TIMEZONESIDKEY,
        NVL(IQ.USER_ROLE_SOURCE_ID, 'Not Applicable') AS USER_ROLE_SOURCE_ID,
        IQ.RECEIVES_INFO_EMAILS,
        IQ.FEDERATION_IDENTIFIER,
        IQ.LAST_IPAD_SYNC,
        IQ.USER_LICENSE,
        IQ.TITLE,
        IQ.PHONE,
        IQ.LAST_LOGIN_DATE,
        IQ.REGION,
        IQ.PROFILE_GROUP_AP,
        IQ.MANAGER_NAME,
        IQ.MANAGER_WWID,
        IQ.ACTIVE_FLAG,
        IQ.MY_ORGANIZATION_CODE,
        IQ.MY_ORGANIZATION_NAME,
        nvl(IQ.ORGANIZATION_L1_CODE, 'Not Available') AS ORGANIZATION_L1_CODE,
        nvl(IQ.ORGANIZATION_L1_NAME, 'Not Available') AS ORGANIZATION_L1_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L2_CODE = IQ.ORGANIZATION_L1_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L2_CODE
                END, 'Not Available') AS ORGANIZATION_L2_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L2_NAME = IQ.ORGANIZATION_L1_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L2_NAME
                END, 'Not Available') AS ORGANIZATION_L2_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L3_CODE = IQ.ORGANIZATION_L2_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L3_CODE
                END, 'Not Available') AS ORGANIZATION_L3_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L3_NAME = IQ.ORGANIZATION_L2_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L3_NAME
                END, 'Not Available') AS ORGANIZATION_L3_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L4_CODE = IQ.ORGANIZATION_L3_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L4_CODE
                END, 'Not Available') AS ORGANIZATION_L4_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L4_NAME = IQ.ORGANIZATION_L3_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L4_NAME
                END, 'Not Available') AS ORGANIZATION_L4_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L5_CODE = IQ.ORGANIZATION_L4_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L5_CODE
                END, 'Not Available') AS ORGANIZATION_L5_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L5_NAME = IQ.ORGANIZATION_L4_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L5_NAME
                END, 'Not Available') AS ORGANIZATION_L5_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L6_CODE = IQ.ORGANIZATION_L5_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L6_CODE
                END, 'Not Available') AS ORGANIZATION_L6_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L6_NAME = IQ.ORGANIZATION_L5_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L6_NAME
                END, 'Not Available') AS ORGANIZATION_L6_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L7_CODE = IQ.ORGANIZATION_L6_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L7_CODE
                END, 'Not Available') AS ORGANIZATION_L7_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L7_NAME = IQ.ORGANIZATION_L6_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L7_NAME
                END, 'Not Available') AS ORGANIZATION_L7_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L8_CODE = IQ.ORGANIZATION_L7_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L8_CODE
                END, 'Not Available') AS ORGANIZATION_L8_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L8_NAME = IQ.ORGANIZATION_L7_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L8_NAME
                END, 'Not Available') AS ORGANIZATION_L8_NAME,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L9_CODE = IQ.ORGANIZATION_L8_CODE
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L9_CODE
                END, 'Not Available') AS ORGANIZATION_L9_CODE,
        nvl(CASE 
                WHEN IQ.ORGANIZATION_L9_NAME = IQ.ORGANIZATION_L8_NAME
                    THEN 'Not Available'
                ELSE IQ.ORGANIZATION_L9_NAME
                END, 'Not Available') AS ORGANIZATION_L9_NAME,
        nvl(IQ.COMMON_ORGANIZATION_L1_CODE, 'Not Available') AS COMMON_ORGANIZATION_L1_CODE,
        nvl(IQ.COMMON_ORGANIZATION_L1_NAME, 'Not Available') AS COMMON_ORGANIZATION_L1_NAME,
        nvl(CASE 
                WHEN IQ.COMMON_ORGANIZATION_L2_CODE = IQ.COMMON_ORGANIZATION_L1_CODE
                    THEN 'Not Available'
                ELSE IQ.COMMON_ORGANIZATION_L2_CODE
                END, 'Not Available') AS COMMON_ORGANIZATION_L2_CODE,
        nvl(CASE 
                WHEN IQ.COMMON_ORGANIZATION_L2_NAME = IQ.COMMON_ORGANIZATION_L1_NAME
                    THEN 'Not Available'
                ELSE IQ.COMMON_ORGANIZATION_L2_NAME
                END, 'Not Available') AS COMMON_ORGANIZATION_L2_NAME,
        nvl(CASE 
                WHEN IQ.COMMON_ORGANIZATION_L3_CODE = IQ.COMMON_ORGANIZATION_L2_CODE
                    THEN 'Not Available'
                ELSE IQ.COMMON_ORGANIZATION_L3_CODE
                END, 'Not Available') AS COMMON_ORGANIZATION_L3_CODE,
        nvl(CASE 
                WHEN IQ.COMMON_ORGANIZATION_L3_NAME = IQ.COMMON_ORGANIZATION_L2_NAME
                    THEN 'Not Available'
                ELSE IQ.COMMON_ORGANIZATION_L3_NAME
                END, 'Not Available') AS COMMON_ORGANIZATION_L3_NAME,
        SYSDATE(),
        SYSDATE(),
        IQ.MSL_PRIMARY_RESPONSIBLE_TA,
        IQ.MSL_SECONDARY_RESPONSIBLE_TA,
        IQ.city,
        IQ.first_name,
        IQ.language_local_key,
        IQ.last_name,
        IQ.manager_source_id,
        IQ.postal_code,
        IQ.STATE,
        IQ.user_type,
        IQ.veeva_user_type,
        IQ.veeva_country_code,
        IQ.shc_user_franchise
    FROM (
        SELECT EMPLOYEE_KEY,
            COUNTRY_CODE,
            EMPLOYEE_SOURCE_ID,
            LAST_MODIFIED_DATE,
            LAST_MODIFIED_BY_ID,
            EMPLOYEE_NAME,
            WWID,
            MOBILE_PHONE,
            EMAIL,
            USER_NAME,
            NICKNAME,
            LOCAL_EMPLOYEE_NUMBER,
            EMPLOYEE_PROFILE_ID AS PROFILE_ID,
            PROFILE_NAME,
            TARGET_VALUE,
            EMPLOYEE_PROFILE_ID,
            COMPANY_NAME,
            DIVISION,
            DEPARTMENT,
            COUNTRY,
            ADDRESS,
            ALIAS,
            TIMEZONESIDKEY,
            USER_ROLE_SOURCE_ID,
            RECEIVES_INFO_EMAILS,
            FEDERATION_IDENTIFIER,
            LAST_IPAD_SYNC,
            USER_LICENSE,
            TITLE,
            PHONE,
            LAST_LOGIN_DATE,
            REGION,
            PROFILE_GROUP_AP,
            MANAGER_NAME,
            MANAGER_WWID,
            ACTIVE_FLAG,
            CASE 
                WHEN MY_ORGANIZATION_CODE = ''
                    THEN 'Not Available'
                ELSE MY_ORGANIZATION_CODE
                END AS MY_ORGANIZATION_CODE,
            CASE 
                WHEN MY_ORGANIZATION_NAME = ''
                    THEN 'Not Available'
                ELSE MY_ORGANIZATION_NAME
                END AS MY_ORGANIZATION_NAME,
            CASE 
                WHEN ORGANIZATION_L1_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L1_CODE
                END AS ORGANIZATION_L1_CODE,
            CASE 
                WHEN ORGANIZATION_L1_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L1_NAME
                END AS ORGANIZATION_L1_NAME,
            CASE 
                WHEN ORGANIZATION_L2_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L2_CODE
                END AS ORGANIZATION_L2_CODE,
            CASE 
                WHEN ORGANIZATION_L2_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L2_NAME
                END AS ORGANIZATION_L2_NAME,
            CASE 
                WHEN ORGANIZATION_L3_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L3_CODE
                END AS ORGANIZATION_L3_CODE,
            CASE 
                WHEN ORGANIZATION_L3_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L3_NAME
                END AS ORGANIZATION_L3_NAME,
            CASE 
                WHEN ORGANIZATION_L4_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L4_CODE
                END AS ORGANIZATION_L4_CODE,
            CASE 
                WHEN ORGANIZATION_L4_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L4_NAME
                END AS ORGANIZATION_L4_NAME,
            CASE 
                WHEN ORGANIZATION_L5_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L5_CODE
                END AS ORGANIZATION_L5_CODE,
            CASE 
                WHEN ORGANIZATION_L5_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L5_NAME
                END AS ORGANIZATION_L5_NAME,
            CASE 
                WHEN ORGANIZATION_L6_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L6_CODE
                END AS ORGANIZATION_L6_CODE,
            CASE 
                WHEN ORGANIZATION_L6_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L6_NAME
                END AS ORGANIZATION_L6_NAME,
            CASE 
                WHEN ORGANIZATION_L7_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L7_CODE
                END AS ORGANIZATION_L7_CODE,
            CASE 
                WHEN ORGANIZATION_L7_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L7_NAME
                END AS ORGANIZATION_L7_NAME,
            CASE 
                WHEN ORGANIZATION_L8_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L8_CODE
                END AS ORGANIZATION_L8_CODE,
            CASE 
                WHEN ORGANIZATION_L8_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L8_NAME
                END AS ORGANIZATION_L8_NAME,
            CASE 
                WHEN ORGANIZATION_L9_CODE = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L9_CODE
                END AS ORGANIZATION_L9_CODE,
            CASE 
                WHEN ORGANIZATION_L9_NAME = ''
                    THEN 'Not Available'
                ELSE ORGANIZATION_L9_NAME
                END AS ORGANIZATION_L9_NAME,
            CASE 
                WHEN COMMON_ORGANIZATION_L1_CODE = ''
                    THEN 'Not Available'
                ELSE COMMON_ORGANIZATION_L1_CODE
                END AS COMMON_ORGANIZATION_L1_CODE,
            CASE 
                WHEN COMMON_ORGANIZATION_L1_NAME = ''
                    THEN 'Not Available'
                ELSE COMMON_ORGANIZATION_L1_NAME
                END AS COMMON_ORGANIZATION_L1_NAME,
            CASE 
                WHEN COMMON_ORGANIZATION_L2_CODE = ''
                    THEN 'Not Available'
                ELSE COMMON_ORGANIZATION_L2_CODE
                END AS COMMON_ORGANIZATION_L2_CODE,
            CASE 
                WHEN COMMON_ORGANIZATION_L2_NAME = ''
                    THEN 'Not Available'
                ELSE COMMON_ORGANIZATION_L2_NAME
                END AS COMMON_ORGANIZATION_L2_NAME,
            CASE 
                WHEN COMMON_ORGANIZATION_L3_CODE = ''
                    THEN 'Not Available'
                ELSE COMMON_ORGANIZATION_L3_CODE
                END AS COMMON_ORGANIZATION_L3_CODE,
            CASE 
                WHEN COMMON_ORGANIZATION_L3_NAME = ''
                    THEN 'Not Available'
                ELSE COMMON_ORGANIZATION_L3_NAME
                END AS COMMON_ORGANIZATION_L3_NAME,
            MSL_PRIMARY_RESPONSIBLE_TA,
            MSL_SECONDARY_RESPONSIBLE_TA,
            city,
            first_name,
            language_local_key,
            last_name,
            manager_source_id,
            postal_code,
            STATE,
            user_type,
            veeva_user_type,
            veeva_country_code,
            shc_user_franchise
        FROM (
            SELECT EQ.*,
                ROW_NUMBER() OVER (
                    PARTITION BY EQ.EMPLOYEE_SOURCE_ID ORDER BY EQ.EMPLOYEE_SOURCE_ID,
                        EQ.RNK,
                        EQ.WWID
                    ) AS ROW_NUM
            FROM wrk_dim_employee_temp EQ
            WHERE LOWER(EQ.USER_LICENSE) != 'not applicable for regional service cloud'
            )
        WHERE ROW_NUM = 1
        ) IQ
    ),
T2
AS (
    SELECT 'Not Applicable',
        'ZZ',
        'Not Applicable',
        SYSDATE(),
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        0,
        'Not Applicable',
        SYSDATE(),
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        SYSDATE(),
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        0,
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        SYSDATE(),
        SYSDATE(),
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable',
        'Not Applicable'
    ),
JOINED
AS (
    SELECT *
    FROM T1
    
    UNION ALL
    
    SELECT *
    FROM T2
    ),
FINAL AS
(
    SELECT 	EMPLOYEE_KEY::VARCHAR(32) AS EMPLOYEE_KEY,
        COUNTRY_CODE::VARCHAR(8) AS COUNTRY_CODE,
        EMPLOYEE_SOURCE_ID::VARCHAR(18) AS EMPLOYEE_SOURCE_ID,
        LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS MODIFIED_DT,
        LAST_MODIFIED_BY_ID::VARCHAR(18) AS MODIFIED_ID,
        EMPLOYEE_NAME::VARCHAR(121) AS EMPLOYEE_NAME,
        WWID::VARCHAR(20) AS EMPLOYEE_WWID,
        MOBILE_PHONE::VARCHAR(40) AS MOBILE_PHONE,
        EMAIL::VARCHAR(128) AS EMAIL_ID,
        USER_NAME::VARCHAR(80) AS USERNAME,
        NICKNAME::VARCHAR(40) AS NICKNAME,
        LOCAL_EMPLOYEE_NUMBER::VARCHAR(20) AS LOCAL_EMPLOYEE_NUMBER,
        PROFILE_ID::VARCHAR(18) AS PROFILE_ID,
        PROFILE_NAME::VARCHAR(255) AS PROFILE_NAME,
        TARGET_VALUE::VARCHAR(255) AS FUNCTION_NAME,
        EMPLOYEE_PROFILE_ID::VARCHAR(18) AS EMPLOYEE_PROFILE_ID,
        COMPANY_NAME::VARCHAR(80) AS COMPANY_NAME,
        DIVISION::VARCHAR(80) AS DIVISION_NAME,
        DEPARTMENT::VARCHAR(80) AS DEPARTMENT_NAME,
        COUNTRY::VARCHAR(80) AS COUNTRY_NAME,
        ADDRESS::VARCHAR(255) AS ADDRESS,
        ALIAS::VARCHAR(18) AS ALIAS,
        TIMEZONESIDKEY::VARCHAR(40) AS TIMEZONESIDKEY,
        USER_ROLE_SOURCE_ID::VARCHAR(18) AS USER_ROLE_SOURCE_ID,
        RECEIVES_INFO_EMAILS::NUMBER(38,0) AS RECEIVES_INFO_EMAILS,
        FEDERATION_IDENTIFIER::VARCHAR(512) AS FEDERATION_IDENTIFIER,
        LAST_IPAD_SYNC::TIMESTAMP_NTZ(9) AS LAST_IPAD_SYNC,
        USER_LICENSE::VARCHAR(1300) AS USER_LICENSE,
        TITLE::VARCHAR(1300) AS TITLE,
        PHONE::VARCHAR(43) AS PHONE,
        LAST_LOGIN_DATE::TIMESTAMP_NTZ(9) AS LAST_LOGIN_DATE,
        REGION::VARCHAR(1300) AS REGION,
        PROFILE_GROUP_AP::VARCHAR(1030) AS PROFILE_GROUP_AP,
        MANAGER_NAME::VARCHAR(80) AS MANAGER_NAME,
        MANAGER_WWID::VARCHAR(18) AS MANAGER_WWID,
        ACTIVE_FLAG::NUMBER(2,0) AS ACTIVE_FLAG,
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
        COMMON_ORGANIZATION_L1_CODE::VARCHAR(18) AS COMMON_ORGANIZATION_L1_CODE,
        COMMON_ORGANIZATION_L1_NAME::VARCHAR(80) AS COMMON_ORGANIZATION_L1_NAME,
        COMMON_ORGANIZATION_L2_CODE::VARCHAR(18) AS COMMON_ORGANIZATION_L2_CODE,
        COMMON_ORGANIZATION_L2_NAME::VARCHAR(80) AS COMMON_ORGANIZATION_L2_NAME,
        COMMON_ORGANIZATION_L3_CODE::VARCHAR(18) AS COMMON_ORGANIZATION_L3_CODE,
        COMMON_ORGANIZATION_L3_NAME::VARCHAR(80) AS COMMON_ORGANIZATION_L3_NAME,
        current_timestamp()::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        current_timestamp()::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        MSL_PRIMARY_RESPONSIBLE_TA::VARCHAR(255) AS MSL_PRIMARY_RESPONSIBLE_TA,
        MSL_SECONDARY_RESPONSIBLE_TA::VARCHAR(255) AS MSL_SECONDARY_RESPONSIBLE_TA,
        CITY::VARCHAR(40) AS CITY,
        FIRST_NAME::VARCHAR(40) AS FIRST_NAME,
        LANGUAGE_LOCAL_KEY::VARCHAR(40) AS LANGUAGE_LOCAL_KEY,
        LAST_NAME::VARCHAR(80) AS LAST_NAME,
        MANAGER_SOURCE_ID::VARCHAR(18) AS MANAGER_SOURCE_ID,
        POSTAL_CODE::VARCHAR(20) AS POSTAL_CODE,
        STATE::VARCHAR(80) AS STATE,
        USER_TYPE::VARCHAR(40) AS USER_TYPE,
        VEEVA_USER_TYPE::VARCHAR(255) AS VEEVA_USER_TYPE,
        VEEVA_COUNTRY_CODE::VARCHAR(255) AS VEEVA_COUNTRY_CODE,
        SHC_USER_FRANCHISE::VARCHAR(255) AS SHC_USER_FRANCHISE
    FROM JOINED
)
SELECT *
FROM FINAL
