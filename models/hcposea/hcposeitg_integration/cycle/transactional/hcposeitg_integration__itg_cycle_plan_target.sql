{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE CYCLE_PLAN_TARGET_SOURCE_ID IN (
                        SELECT CYCLE_PLAN_TARGET_SOURCE_ID
                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_cycle_plan_target') }} STG_CYC_TAR
                        WHERE STG_CYC_TAR.CYCLE_PLAN_TARGET_SOURCE_ID = CYCLE_PLAN_TARGET_SOURCE_ID
                        AND CYCLE_PLAN_VOD_SOURCE_ID IN (
                            SELECT CYCLE_PLAN_SOURCE_ID
                            FROM {{ ref('hcposeitg_integration__itg_cycle_plan') }}
                            )
                        )
                    AND COUNTRY_CODE IN (
                        SELECT COUNTRY_CODE
                        FROM {{ ref('hcposeitg_integration__itg_cycle_plan') }}
                        WHERE CYCLE_PLAN_SOURCE_ID IN (
                            SELECT CYCLE_PLAN_VOD_SOURCE_ID
                            FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_cycle_plan_target') }}
                            )
                        );
                    {% endif %}
                    "
    )
}}

WITH sdl_hcp_osea_cycle_plan_target
AS (
  SELECT *
  FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_cycle_plan_target') }}
  ),
itg_cycle_plan
AS (
  SELECT *
  FROM {{ ref('hcposeitg_integration__itg_cycle_plan') }}
  ),
trns
AS (
  SELECT STG.CYCLE_PLAN_TARGET_SOURCE_ID,
    (
      CASE 
        WHEN UPPER(STG.IS_DELETED) = 'TRUE'
          THEN 1
        WHEN UPPER(STG.IS_DELETED) = 'FALSE'
          THEN 0
        ELSE 0
        END
      ) AS IS_DELETED,
    STG.CYCLE_PLAN_TARGET_NAME,
    STG.CREATED_DATE,
    STG.CREATED_BY_ID,
    STG.LAST_MODIFIED_DATE,
    STG.LAST_MODIFIED_BY_ID,
    STG.CYCLE_PLAN_VOD_SOURCE_ID,
    STG.ACTUAL_CALLS,
    STG.ATTAINMENT,
    STG.CYCLE_PLAN_ACCOUNT_SOURCE_ID,
    STG.ORIGINAL_PLANNED_CALLS,
    STG.PLANNED_CALLS,
    STG.TOTAL_ACTUAL_CALLS,
    STG.TOTAL_ATTAINMENT,
    STG.TOTAL_PLANNED_CALLS,
    STG.EXTERNAL_ID,
    STG.SCHEDULED_CALLS,
    STG.TOTAL_SCHEDULED_CALLS,
    STG.REMAINING,
    TOTAL_REMAINING,
    STG.REMAINING_SCHEDULE,
    STG.TOTAL_REMAINING_SCHEDULE,
    STG.PRIMARY_PARENT_NAME,
    STG.SPECIALTY_1,
    STG.ACCOUNT_SOURCE_ID,
    STG.CPT_CFA_100,
    STG.CPT_CFA_66,
    STG.CPT_CFA_33,
    STG.NUMBER_OF_CFA_100_DETAILS,
    STG.NUMBER_OF_PRODUCT_DETAILS,
    ITG.COUNTRY_CODE,
    JJ_AC_CLASSIFICATION__C,
    CASE 
      WHEN UPPER(STG.IS_LOCKED) = 'TRUE'
        THEN 1
      WHEN UPPER(STG.IS_LOCKED) = 'FALSE'
        THEN 0
      ELSE 0
      END AS IS_LOCKED,
    TARGET_REACHED_FLAG,
    SYSDATE() AS inserted_date,
    NULL AS updated_date
  FROM sdl_hcp_osea_cycle_plan_target STG
  JOIN itg_cycle_plan ITG ON ITG.CYCLE_PLAN_SOURCE_ID = STG.CYCLE_PLAN_VOD_SOURCE_ID
  ),
final
AS (
  SELECT cycle_plan_target_source_id::VARCHAR(18) AS cycle_plan_target_source_id,
    is_deleted::VARCHAR(5) AS is_deleted,
    cycle_plan_target_name::VARCHAR(80) AS cycle_plan_target_name,
    created_date::timestamp_ntz(9) AS created_date,
    created_by_id::VARCHAR(18) AS created_by_id,
    last_modified_date::timestamp_ntz(9) AS last_modified_date,
    last_modified_by_id::VARCHAR(18) AS last_modified_by_id,
    cycle_plan_vod_source_id::VARCHAR(18) AS cycle_plan_vod_source_id,
    actual_calls::number(3, 0) AS actual_calls,
    attainment::number(18, 0) AS attainment,
    cycle_plan_account_source_id::VARCHAR(18) AS cycle_plan_account_source_id,
    original_planned_calls::number(3, 0) AS original_planned_calls,
    planned_calls::number(3, 0) AS planned_calls,
    total_actual_calls::number(5, 0) AS total_actual_calls,
    total_attainment::number(18, 0) AS total_attainment,
    total_planned_calls::number(5, 0) AS total_planned_calls,
    external_id::VARCHAR(100) AS external_id,
    scheduled_calls::number(18, 0) AS scheduled_calls,
    total_scheduled_calls::number(5, 0) AS total_scheduled_calls,
    remaining::number(18, 0) AS remaining,
    total_remaining::number(18, 0) AS total_remaining,
    remaining_schedule::number(18, 0) AS remaining_schedule,
    total_remaining_schedule::number(18, 0) AS total_remaining_schedule,
    primary_parent_name::VARCHAR(1300) AS primary_parent_name,
    specialty_1::VARCHAR(1300) AS specialty_1,
    account_source_id::VARCHAR(1300) AS account_source_id,
    cpt_cfa_100::number(18, 0) AS cpt_cfa_100,
    cpt_cfa_66::number(18, 0) AS cpt_cfa_66,
    cpt_cfa_33::number(18, 0) AS cpt_cfa_33,
    number_of_cfa_100_details::number(18, 0) AS number_of_cfa_100_details,
    number_of_product_details::number(18, 0) AS number_of_product_details,
    country_code::VARCHAR(2) AS country_code,
    JJ_AC_CLASSIFICATION__C::VARCHAR(1300) AS classification,
    is_locked::number(38, 0) AS is_locked,
    target_reached_flag::number(18, 0) AS target_reached_flag,
    inserted_date::timestamp_ntz(9) AS inserted_date,
    updated_date::timestamp_ntz(9) AS updated_date
  FROM trns
  )
SELECT *
FROM final