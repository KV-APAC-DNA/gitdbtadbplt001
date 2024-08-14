WITH wrk_dim_employee_temp
AS (
    SELECT *
    FROM oseedw_integration.wrk_dim_employee_temp
    ),
t1
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
    
    UNION ALL
    
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
final as
(
    select EMPLOYEE_KEY VARCHAR(32) as ,
        COUNTRY_CODE VARCHAR(8) as ,
        EMPLOYEE_SOURCE_ID VARCHAR(18) as ,
        MODIFIED_DT TIMESTAMP_NTZ(9) as ,
        MODIFIED_ID VARCHAR(18) as ,
        EMPLOYEE_NAME VARCHAR(121) as ,
        EMPLOYEE_WWID VARCHAR(20) as ,
        MOBILE_PHONE VARCHAR(40) as ,
        EMAIL_ID VARCHAR(128) as ,
        USERNAME VARCHAR(80) as ,
        NICKNAME VARCHAR(40) as ,
        LOCAL_EMPLOYEE_NUMBER VARCHAR(20) as ,
        PROFILE_ID VARCHAR(18) as ,
        PROFILE_NAME VARCHAR(255) as ,
        FUNCTION_NAME VARCHAR(255) as ,
        EMPLOYEE_PROFILE_ID VARCHAR(18) as ,
        COMPANY_NAME VARCHAR(80) as ,
        DIVISION_NAME VARCHAR(80) as ,
        DEPARTMENT_NAME VARCHAR(80) as ,
        COUNTRY_NAME VARCHAR(80) as ,
        ADDRESS VARCHAR(255) as ,
        ALIAS VARCHAR(18) as ,
        TIMEZONESIDKEY VARCHAR(40) as ,
        USER_ROLE_SOURCE_ID VARCHAR(18) as ,
        RECEIVES_INFO_EMAILS NUMBER(38,0) as ,
        FEDERATION_IDENTIFIER VARCHAR(512) as ,
        LAST_IPAD_SYNC TIMESTAMP_NTZ(9) as ,
        USER_LICENSE VARCHAR(1300) as ,
        TITLE VARCHAR(1300) as ,
        PHONE VARCHAR(43) as ,
        LAST_LOGIN_DATE TIMESTAMP_NTZ(9) as ,
        REGION VARCHAR(1300) as ,
        PROFILE_GROUP_AP VARCHAR(1030) as ,
        MANAGER_NAME VARCHAR(80) as ,
        MANAGER_WWID VARCHAR(18) as ,
        ACTIVE_FLAG NUMBER(2,0) as ,
        MY_ORGANIZATION_CODE VARCHAR(18) as ,
        MY_ORGANIZATION_NAME VARCHAR(80) as ,
        ORGANIZATION_L1_CODE VARCHAR(18) as ,
        ORGANIZATION_L1_NAME VARCHAR(80) as ,
        ORGANIZATION_L2_CODE VARCHAR(18) as ,
        ORGANIZATION_L2_NAME VARCHAR(80) as ,
        ORGANIZATION_L3_CODE VARCHAR(18) as ,
        ORGANIZATION_L3_NAME VARCHAR(80) as ,
        ORGANIZATION_L4_CODE VARCHAR(18) as ,
        ORGANIZATION_L4_NAME VARCHAR(80) as ,
        ORGANIZATION_L5_CODE VARCHAR(18) as ,
        ORGANIZATION_L5_NAME VARCHAR(80) as ,
        ORGANIZATION_L6_CODE VARCHAR(18) as ,
        ORGANIZATION_L6_NAME VARCHAR(80) as ,
        ORGANIZATION_L7_CODE VARCHAR(18) as ,
        ORGANIZATION_L7_NAME VARCHAR(80) as ,
        ORGANIZATION_L8_CODE VARCHAR(18) as ,
        ORGANIZATION_L8_NAME VARCHAR(80) as ,
        ORGANIZATION_L9_CODE VARCHAR(18) as ,
        ORGANIZATION_L9_NAME VARCHAR(80) as ,
        COMMON_ORGANIZATION_L1_CODE VARCHAR(18) as ,
        COMMON_ORGANIZATION_L1_NAME VARCHAR(80) as ,
        COMMON_ORGANIZATION_L2_CODE VARCHAR(18) as ,
        COMMON_ORGANIZATION_L2_NAME VARCHAR(80) as ,
        COMMON_ORGANIZATION_L3_CODE VARCHAR(18) as ,
        COMMON_ORGANIZATION_L3_NAME VARCHAR(80) as ,
        INSERTED_DATE TIMESTAMP_NTZ(9) as ,
        UPDATED_DATE TIMESTAMP_NTZ(9) as ,
        MSL_PRIMARY_RESPONSIBLE_TA VARCHAR(255) as ,
        MSL_SECONDARY_RESPONSIBLE_TA VARCHAR(255) as ,
        CITY VARCHAR(40) as ,
        FIRST_NAME VARCHAR(40) as ,
        LANGUAGE_LOCAL_KEY VARCHAR(40) as ,
        LAST_NAME VARCHAR(80) as ,
        MANAGER_SOURCE_ID VARCHAR(18) as ,
        POSTAL_CODE VARCHAR(20) as ,
        STATE VARCHAR(80) as ,
        USER_TYPE VARCHAR(40) as ,
        VEEVA_USER_TYPE VARCHAR(255) as ,
        VEEVA_COUNTRY_CODE VARCHAR(255) as ,
        SHC_USER_FRANCHISE VARCHAR(255) as ,
    from t1
),
SELECT *
FROM FINAL
