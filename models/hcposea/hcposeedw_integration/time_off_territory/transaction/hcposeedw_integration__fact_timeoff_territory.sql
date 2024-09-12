WITH itg_time_off_territory
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_time_off_territory') }}
    ),
dim_employee
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_employee') }}
    ),
dim_date
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_date') }}
    ),
dim_country
AS (
    SELECT *
    FROM {{ ref('hcposeedw_integration__dim_country') }}
    ),
itg_lookup_retention_period
AS (
    SELECT *
    FROM {{ source('hcposeitg_integration', 'itg_lookup_retention_period') }}
    ),
T1
AS (
    SELECT COUN.country_key,
        NVL(US.EMPLOYEE_KEY, 'Not Applicable') as EMPLOYEE_KEY, 
        D1.date_key AS START_DATE_KEY,
        CASE 
            WHEN D2.date_value <= D1.date_value
                THEN D1.date_key
            ELSE D2.date_key
            END AS END_DATE_KEY,
        TERRITORY_NAME AS TERRITORY_NAME,
        REASON AS REASON_TYPE,
        WORKING_HOURS_OFF as HOURS_OFF,
        TIME_TYPE,
        START_TIME,
        WORKING_HOURS_ON as HOURS,
        SIMP_DESCRIPTION AS COMMENTS,
        SIMP_TIME_ON_TIME_OFF AS SEA_TIME_ON_TIME_OFF,
        SIMP_FRML_HOURS_ON AS SEA_FRML_HOURS_ON,
        SIMP_FRML_NON_WORKING_HOURS_OFF AS SEA_FRML_NON_WORKING_HOURS_OFF,
        MOBILE_ID,
        FRML_TOTAL_WORK_DAYS AS SEA_FRML_TOTAL_WORK_DAYS,
        FRML_PLANNED_WORK_DAYS AS SEA_FRML_PLANNED_WORK_DAYS,
        TIME_ON,
        TOT.USER_NAME,
        TOT.USER_PROFILE,
        CALCULATEDHOURS_OFF,
        TOTAL_TIME_OFF,
        SM_REASON,
        TOT_NAME,
        TOT_SOURCE_ID TIMEOFF_TERRITORY_SOURCE_ID,
        TOT.LAST_MODIFIED_DATE AS MODIFY_DT,
        TOT.LAST_MODIFIED_BY_ID AS MODIFY_ID,
        current_timestamp() AS INSERTED_DATE,
        current_timestamp() AS UPDATED_DATE
    FROM itg_time_off_territory TOT,
        dim_employee US,
        dim_date D1,
        dim_date D2,
        dim_country COUN
    WHERE TOT.OWNER_SOURCE_ID = US.EMPLOYEE_SOURCE_ID(+)
        --AND   TOT.LOAD_FLAG = 'N' 
        AND TOT.COUNTRY_CODE = US.COUNTRY_CODE(+)
        AND TOT.COUNTRY_CODE = COUN.COUNTRY_CODE
        AND TOT.IS_DELETED = 0
        AND D1.date_value = TOT.TOT_DATE
        AND D2.date_value = nvl((DATEADD('DAY', CAST((nvl(TOT.WORKING_HOURS_ON, 0) + nvl(simp_frml_non_working_hours_off, 0)) / 8 AS INTEGER), TOT.TOT_DATE) - 1), TOT.TOT_DATE)
        AND TOT_DATE > (
            SELECT DATE_TRUNC(year, DATEADD(DAY,- retention_years * 365 ,current_timestamp() ))
            FROM itg_lookup_retention_period
            WHERE upper(table_name) = 'FACT_TIMEOFF_TERRITORY'
            )
    ),
FINAL
AS (
    SELECT     COUNTRY_KEY::VARCHAR(8) as COUNTRY_KEY,
        EMPLOYEE_KEY::VARCHAR(32) as EMPLOYEE_KEY,
        START_DATE_KEY::VARCHAR(32) as START_DATE_KEY,
        END_DATE_KEY::VARCHAR(32) as END_DATE_KEY,
        TERRITORY_NAME::VARCHAR(255) as TERRITORY_NAME,
        REASON_TYPE::VARCHAR(255) as REASON_TYPE,
        HOURS_OFF::VARCHAR(255) as HOURS_OFF,
        TIME_TYPE::VARCHAR(255) as TIME_TYPE,
        START_TIME::VARCHAR(255) as START_TIME,
        HOURS::NUMBER(18,0) as HOURS,
        COMMENTS::VARCHAR(800) as COMMENTS,
        SEA_TIME_ON_TIME_OFF::VARCHAR(255) as SEA_TIME_ON_TIME_OFF,
        SEA_FRML_HOURS_ON::NUMBER(18,0) as SEA_FRML_HOURS_ON,
        SEA_FRML_NON_WORKING_HOURS_OFF::NUMBER(18,0) as SEA_FRML_NON_WORKING_HOURS_OFF,
        MOBILE_ID::VARCHAR(103) as MOBILE_ID,
        SEA_FRML_TOTAL_WORK_DAYS::NUMBER(18,0) as SEA_FRML_TOTAL_WORK_DAYS,
        SEA_FRML_PLANNED_WORK_DAYS::NUMBER(18,0) as SEA_FRML_PLANNED_WORK_DAYS,
        TIME_ON::VARCHAR(255) as TIME_ON,
        USER_NAME::VARCHAR(255) as USER_NAME,
        USER_PROFILE::VARCHAR(1030) as USER_PROFILE,
        CALCULATEDHOURS_OFF::NUMBER(18,0) as CALCULATEDHOURS_OFF,
        TOTAL_TIME_OFF::NUMBER(18,0) as TOTAL_TIME_OFF,
        SM_REASON::VARCHAR(255) as SM_REASON,
        TOT_NAME::VARCHAR(80) as TOT_NAME,
        TIMEOFF_TERRITORY_SOURCE_ID::VARCHAR(18) as TIMEOFF_TERRITORY_SOURCE_ID,
        MODIFY_DT::TIMESTAMP_NTZ(9) as MODIFY_DT,
        MODIFY_ID::VARCHAR(18) as MODIFY_ID,
        INSERTED_DATE::TIMESTAMP_NTZ(9) as INSERTED_DATE,
        UPDATED_DATE::TIMESTAMP_NTZ(9) as UPDATED_DATE

    FROM T1
    )
SELECT *
FROM FINAL
