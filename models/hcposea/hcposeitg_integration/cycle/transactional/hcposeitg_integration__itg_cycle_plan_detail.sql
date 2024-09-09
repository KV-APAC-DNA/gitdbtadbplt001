{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE CYCLE_PLAN_DETAIL_SOURCE_ID IN (
                        SELECT CYCLE_PLAN_DETAIL_SOURCE_ID
                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_cycle_plan_detail') }} STG_CYC_PLAN
                        WHERE STG_CYC_PLAN.CYCLE_PLAN_DETAIL_SOURCE_ID = CYCLE_PLAN_DETAIL_SOURCE_ID
                        AND CYCLE_PLAN_TARGET_SOURCE_ID IN (
                            SELECT CYCLE_PLAN_TARGET_SOURCE_ID
                            FROM {{ ref('hcposeitg_integration__itg_cycle_plan_target') }}
                            )
                        )
                    AND COUNTRY_CODE IN (
                        SELECT COUNTRY_CODE
                        FROM {{ ref('hcposeitg_integration__itg_cycle_plan_target') }}
                        WHERE CYCLE_PLAN_TARGET_SOURCE_ID IN (
                            SELECT CYCLE_PLAN_TARGET_SOURCE_ID
                            FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_cycle_plan_detail') }}
                            )
                        );
                    {% endif %}
                    "
    )
}}

WITH sdl_hcp_osea_cycle_plan_detail
AS (
  SELECT *
  FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_cycle_plan_detail') }}
  ),
itg_cycle_plan_target
AS (
  SELECT *
  FROM {{ ref('hcposeitg_integration__itg_cycle_plan_target') }}
  ),
trns
AS (
  SELECT STG.CYCLE_PLAN_DETAIL_SOURCE_ID,
    (
      CASE 
        WHEN UPPER(STG.IS_DELETED) = 'TRUE'
          THEN 1
        WHEN UPPER(STG.IS_DELETED) = 'FALSE'
          THEN 0
        ELSE 0
        END
      ) AS IS_DELETED,
    CYCLE_PLAN_DETAIL_NAME,
    STG.CREATED_DATE,
    STG.CREATED_BY_ID,
    STG.LAST_MODIFIED_DATE,
    STG.LAST_MODIFIED_BY_ID,
    STG.CYCLE_PLAN_TARGET_SOURCE_ID,
    STG.ACTUAL_DETAILS,
    STG.ATTAINMENT,
    STG.PLANNED_DETAILS,
    STG.PRODUCT_SOURCE_ID,
    STG.SCHEDULED_DETAILS,
    STG.TOTAL_ACTUAL_DETAILS,
    STG.TOTAL_ATTAINMENT,
    STG.TOTAL_PLANNED_DETAILS,
    STG.TOTAL_SCHEDULED_DETAILS,
    STG.REMAINING,
    STG.TOTAL_REMAINING,
    STG.CLASSIFICATION_TYPE,
    STG.CFA_100,
    STG.CFA_33,
    STG.CFA_66,
    ITG.COUNTRY_CODE,
    CASE 
      WHEN UPPER(STG.IS_LOCKED) = 'TRUE'
        THEN 1
      WHEN UPPER(STG.IS_LOCKED) = 'FALSE'
        THEN 0
      ELSE 0
      END AS IS_LOCKED,
    current_timestamp() AS inserted_date,
    NULL AS updated_date,
    STG.Adoption_Style
  FROM sdl_hcp_OSEA_CYCLE_PLAN_DETAIL STG
  JOIN itg_CYCLE_PLAN_TARGET ITG ON ITG.CYCLE_PLAN_TARGET_SOURCE_ID = STG.CYCLE_PLAN_TARGET_SOURCE_ID
  ),
final
AS (
  SELECT cycle_plan_detail_source_id::VARCHAR(18) AS cycle_plan_detail_source_id,
    is_deleted::VARCHAR(5) AS is_deleted,
    cycle_plan_detail_name::VARCHAR(80) AS cycle_plan_detail_name,
    created_date::timestamp_ntz(9) AS created_date,
    created_by_id::VARCHAR(18) AS created_by_id,
    last_modified_date::timestamp_ntz(9) AS last_modified_date,
    last_modified_by_id::VARCHAR(18) AS last_modified_by_id,
    cycle_plan_target_source_id::VARCHAR(18) AS cycle_plan_target_source_id,
    actual_details::number(3, 0) AS actual_details,
    attainment::number(18, 0) AS attainment,
    planned_details::number(3, 0) AS planned_details,
    product_source_id::VARCHAR(18) AS product_source_id,
    scheduled_details::number(3, 0) AS scheduled_details,
    total_actual_details::number(5, 0) AS total_actual_details,
    total_attainment::number(18, 0) AS total_attainment,
    total_planned_details::number(5, 0) AS total_planned_details,
    total_scheduled_details::number(5, 0) AS total_scheduled_details,
    remaining::number(18, 0) AS remaining,
    total_remaining::number(18, 0) AS total_remaining,
    classification_type::VARCHAR(255) AS classification_type,
    cfa_100::number(18, 0) AS cfa_100,
    cfa_33::number(18, 0) AS cfa_33,
    cfa_66::number(18, 0) AS cfa_66,
    country_code::VARCHAR(2) AS country_code,
    is_locked::number(38, 0) AS is_locked,
    inserted_date::timestamp_ntz(9) AS inserted_date,
    updated_date::timestamp_ntz(9) AS updated_date,
    adoption_style::VARCHAR(255) AS adoption_style
  FROM trns
  )
SELECT *
FROM final