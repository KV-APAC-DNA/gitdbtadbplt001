{{
    config
    (
        materialized='incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE PROFILE_KEY IN (
                        SELECT PROFILE_KEY
                        FROM {{ ref('hcposeitg_integration__itg_profile') }} ITG_PROF
                        WHERE ITG_PROF.PROFILE_KEY = PROFILE_KEY
                        );
                    DELETE
                    FROM {{this}}
                    WHERE PROFILE_KEY = 'Not Applicable';
                    {% endif %}
                    "
    )
}}


WITH itg_lookup_eng_data
AS (
  SELECT *
  FROM {{ source('hcposeitg_integration', 'itg_lookup_eng_data') }}
  ),
itg_profile
AS (
  SELECT *
  FROM {{ ref('hcposeitg_integration__itg_profile') }}
  ),
prof_func_name
AS (
  SELECT COUNTRY_CODE,
    KEY_VALUE,
    TARGET_VALUE
  FROM itg_lookup_eng_data
  WHERE UPPER(TABLE_NAME) = 'DIM_PROFILE'
    AND UPPER(COLUMN_NAME) = 'PROFILE_NAME'
    AND UPPER(TARGET_COLUMN_NAME) = 'FUNCTION_NAME'
  ),
union1
AS (
  SELECT DISTINCT PROFILE_KEY as profile_key,
    ITG_PROF.COUNTRY_CODE as country_code,
    NVL(PROFILE_SOURCE_ID, 'Not Applicable') as profile_source_id,
    LAST_MODIFIED_DATE as modified_dt,
    NVL(LAST_MODIFIED_BY_ID, 'Not Applicable') as modified_id,
    PROFILE_NAME as profile_name,
    PROF_FUNC_NAME.TARGET_VALUE AS FUNCTION_NAME,
    CREATED_DATE as created_date,
    NVL(CREATED_BY_ID, 'Not Applicable') as created_by_id,
    TYPE as type,
    NVL(USERLICENSE_SOURCE_ID, 'Not Applicable') as userlicense_source_id,
    USERTYPE as usertype,
    DESCRIPTION as description,
    current_timestamp() AS inserted_date,
    current_timestamp() AS updated_date
  FROM itg_PROFILE ITG_PROF
  LEFT OUTER JOIN PROF_FUNC_NAME ON ITG_PROF.PROFILE_NAME = PROF_FUNC_NAME.KEY_VALUE
    AND ITG_PROF.COUNTRY_CODE = PROF_FUNC_NAME.COUNTRY_CODE
  ),
union2
AS (
  SELECT 'Not Applicable' AS profile_key,
    'ZZ' AS country_code,
    'Not Applicable' AS profile_source_id,
    current_timestamp() AS modified_dt,
    'Not Applicable' AS modified_id,
    'Not Applicable' AS profile_name,
    'Not Applicable' AS function_name,
    current_timestamp() AS created_date,
    'Not Applicable' AS created_by_id,
    'Not Applicable' AS type,
    'Not Applicable' AS userlicense_source_id,
    'Not Applicable' AS usertype,
    'Not Applicable' AS description,
    current_timestamp() AS inserted_date,
    current_timestamp() AS updated_date
  ),
trns
AS (
  SELECT *
  FROM union1
  
  UNION ALL
  
  SELECT *
  FROM union2
  ),
final
AS (
  SELECT profile_key::VARCHAR(32) AS profile_key,
    country_code::VARCHAR(2) AS country_code,
    profile_source_id::VARCHAR(18) AS profile_source_id,
    modified_dt::timestamp_ntz(9) AS modified_dt,
    modified_id::VARCHAR(18) AS modified_id,
    profile_name::VARCHAR(255) AS profile_name,
    function_name::VARCHAR(255) AS function_name,
    created_date::timestamp_ntz(9) AS created_date,
    created_by_id::VARCHAR(30) AS created_by_id,
    type::VARCHAR(43) AS type,
    userlicense_source_id::VARCHAR(18) AS userlicense_source_id,
    usertype::VARCHAR(43) AS usertype,
    description::VARCHAR(255) AS description,
    inserted_date::timestamp_ntz(9) AS inserted_date,
    updated_date::timestamp_ntz(9) AS updated_date
  FROM trns
  )
SELECT *
FROM final