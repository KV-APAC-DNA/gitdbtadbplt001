{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE FROM {{this}}
                    WHERE (TOT_SOURCE_ID) IN (SELECT TOT_SOURCE_ID
                    FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_time_off_territory') }} STG_TOT
                    WHERE STG_TOT.TOT_SOURCE_ID = TOT_SOURCE_ID);
                    {% endif %}
                    "
    )
}}

WITH sdl_hcp_osea_time_off_territory
AS (
  SELECT *
  FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_time_off_territory') }}
  ),
itg_user
AS (
  SELECT *
  FROM {{ ref('hcposeitg_integration__itg_user') }}
  ),
trns
AS (
  SELECT T.TOT_SOURCE_ID,
    T.OWNER_SOURCE_ID,
    (
      CASE 
        WHEN UPPER(T.IS_DELETED) = 'TRUE'
          THEN 1
        WHEN UPPER(T.IS_DELETED) IS NULL
          THEN 0
        WHEN UPPER(T.IS_DELETED) = ' '
          THEN 0
        ELSE 0
        END
      ) AS IS_DELETED,
    T.TOT_NAME,
    T.RECORD_TYPE_SOURCE_ID,
    T.CREATED_DATE,
    T.CREATED_BY_ID,
    T.LAST_MODIFIED_DATE,
    T.LAST_MODIFIED_BY_ID,
    T.REASON,
    T.TERRITORY_NAME,
    T.TOT_DATE,
    T.STATUS_CD,
    T.TIME_TYPE,
    T.WORKING_HOURS_ON,
    T.MOBILE_ID,
    T.WORKING_HOURS_OFF,
    T.START_TIME,
    T.SIMP_TIME_ON_TIME_OFF,
    T.SIMP_FRML_HOURS_ON,
    T.FRML_TOTAL_WORK_DAYS,
    T.SIMP_FRML_NON_WORKING_HOURS_OFF,
    T.FRML_PLANNED_WORK_DAYS,
    T.SIMP_DESCRIPTION,
    T.TIME_ON,
    T.USER_NAME,
    T.USER_PROFILE,
    T.CALCULATEDHOURS_OFF,
    T.TOTAL_TIME_OFF,
    T.SM_REASON,
    nvl(U.COUNTRY_CODE, 'ZZ') COUNTRY_CODE,
    SYSDATE() AS inserted_date,
    SYSDATE() AS updated_date,
    T.Approval_Status,
    T.Owners_Manager_Email_id
  FROM sdl_hcp_osea_time_off_territory T,
    itg_user U
  WHERE T.OWNER_SOURCE_ID = U.EMPLOYEE_SOURCE_ID(+)
    --To remove duplicate records due to user_rg----
    AND LOWER(U.USER_LICENSE) != 'not applicable for regional service cloud'
  ),
final
AS (
  SELECT tot_source_id::VARCHAR(18) AS tot_source_id,
    owner_source_id::VARCHAR(18) AS owner_source_id,
    is_deleted::number(38, 0) AS is_deleted,
    tot_name::VARCHAR(80) AS tot_name,
    record_type_source_id::VARCHAR(18) AS record_type_source_id,
    created_date::timestamp_ntz(9) AS created_date,
    created_by_id::VARCHAR(18) AS created_by_id,
    last_modified_date::timestamp_ntz(9) AS last_modified_date,
    last_modified_by_id::VARCHAR(18) AS last_modified_by_id,
    reason::VARCHAR(255) AS reason,
    territory_name::VARCHAR(255) AS territory_name,
    tot_date::DATE AS tot_date,
    status_cd::VARCHAR(255) AS status_cd,
    time_type::VARCHAR(255) AS time_type,
    working_hours_on::number(18, 0) AS working_hours_on,
    mobile_id::VARCHAR(100) AS mobile_id,
    working_hours_off::VARCHAR(255) AS working_hours_off,
    start_time::VARCHAR(255) AS start_time,
    simp_time_on_time_off::VARCHAR(255) AS simp_time_on_time_off,
    simp_frml_hours_on::number(18, 0) AS simp_frml_hours_on,
    frml_total_work_days::number(18, 0) AS frml_total_work_days,
    simp_frml_non_working_hours_off::number(18, 0) AS simp_frml_non_working_hours_off,
    frml_planned_work_days::number(18, 0) AS frml_planned_work_days,
    simp_description::VARCHAR(800) AS simp_description,
    time_on::VARCHAR(255) AS time_on,
    user_name::VARCHAR(18) AS user_name,
    user_profile::VARCHAR(1300) AS user_profile,
    sm_reason::VARCHAR(255) AS sm_reason,
    calculatedhours_off::number(18, 0) AS calculatedhours_off,
    total_time_off::number(18, 0) AS total_time_off,
    country_code::VARCHAR(10) AS country_code,
    inserted_date::timestamp_ntz(9) AS inserted_date,
    updated_date::timestamp_ntz(9) AS updated_date,
    approval_status::VARCHAR(255) AS approval_status,
    owners_manager_email_id::VARCHAR(80) AS owners_manager_email_id
  FROM trns
  )
SELECT *
FROM final
