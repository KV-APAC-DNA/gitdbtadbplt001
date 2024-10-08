{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook = "{% if is_incremental() %}
                    DELETE FROM {{this}}
                    WHERE (CALL_DISCUSSION_SOURCE_ID) IN (SELECT STG.CALL_DISCUSSION_SOURCE_ID
                                                    FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_discussion') }} STG
                                                    WHERE STG.CALL_DISCUSSION_SOURCE_ID = CALL_DISCUSSION_SOURCE_ID
                                                    AND CALL_SOURCE_ID IN (SELECT CALL_SOURCE_ID FROM {{ ref('hcposeitg_integration__itg_call') }}))
                    AND COUNTRY_CODE IN (SELECT UPPER(COUNTRY_CODE) FROM {{ ref('hcposeitg_integration__itg_call') }} WHERE  CALL_SOURCE_ID IN (SELECT CALL_SOURCE_ID FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_discussion') }})) ;
                    {% endif %}"
    )
}}

WITH sdl_hcp_osea_call_discussion
AS (
    SELECT *
    FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_discussion') }}
    ),
itg_CALL
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_call') }}
    ),
T1
AS (
    SELECT STG.CALL_DISCUSSION_SOURCE_ID,
       (CASE WHEN UPPER(STG.IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(STG.IS_DELETED) IS NULL THEN 0 WHEN UPPER(STG.IS_DELETED) = ' ' THEN 0 ELSE 0 END) AS IS_DELETED,
       CALL_DISCUSSION_NAME,
       STG.RECORD_TYPE_SOURCE_ID,
       STG.CREATED_DATE,
       STG.CREATED_BY_ID,
       STG.LAST_MODIFIED_DATE,
       STG.LAST_MODIFIED_BY_ID,
       STG.ACCOUNT_SOURCE_ID,
       STG.CALL_SOURCE_ID,
       STG.COMMENTS,
       STG.PRODUCT_SOURCE_ID,
       STG.DISCUSSION_TOPICS,
       STG.MEDICAL_EVENT,
       STG.IS_PARENT_CALL,
       STG.DISCUSSION_TYPE,
       STG.CALL_DISCUSSION_TYPE,
       STG.EFFECTIVENESS,
       STG.FOLLOW_UP_ACTIVITY,
       STG.OUTCOMES,
       STG.FOLLOW_UP_ADDITIONAL_INFO,
       STG.FOLLOW_UP_DATE,
       STG.MATERIALS_USED,
       ITG.COUNTRY_CODE,
       STG.Call_date,
       STG.Detail_Group_source_id,
       STG.User_source_id
    FROM sdl_hcp_OSEA_CALL_DISCUSSION STG
    JOIN itg_CALL ITG ON ITG.CALL_SOURCE_ID = STG.CALL_SOURCE_ID
    ),
FINAL AS
(
    SELECT CALL_DISCUSSION_SOURCE_ID::VARCHAR(18) AS CALL_DISCUSSION_SOURCE_ID,
        IS_DELETED::VARCHAR(5) AS IS_DELETED,
        CALL_DISCUSSION_NAME::VARCHAR(80) AS CALL_DISCUSSION_NAME,
        RECORD_TYPE_SOURCE_ID::VARCHAR(18) AS RECORD_TYPE_SOURCE_ID,
        CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
        CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
        LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
        ACCOUNT_SOURCE_ID::VARCHAR(18) AS ACCOUNT_SOURCE_ID,
        CALL_SOURCE_ID::VARCHAR(18) AS CALL_SOURCE_ID,
        COMMENTS::VARCHAR(800) AS COMMENTS,
        PRODUCT_SOURCE_ID::VARCHAR(18) AS PRODUCT_SOURCE_ID,
        DISCUSSION_TOPICS::VARCHAR(255) AS DISCUSSION_TOPICS,
        MEDICAL_EVENT::VARCHAR(18) AS MEDICAL_EVENT,
        IS_PARENT_CALL::NUMBER(18,0) AS IS_PARENT_CALL,
        DISCUSSION_TYPE::VARCHAR(255) AS DISCUSSION_TYPE,
        CALL_DISCUSSION_TYPE::VARCHAR(255) AS CALL_DISCUSSION_TYPE,
        EFFECTIVENESS::VARCHAR(255) AS EFFECTIVENESS,
        FOLLOW_UP_ACTIVITY::VARCHAR(255) AS FOLLOW_UP_ACTIVITY,
        OUTCOMES::VARCHAR(255) AS OUTCOMES,
        FOLLOW_UP_ADDITIONAL_INFO::VARCHAR(800) AS FOLLOW_UP_ADDITIONAL_INFO,
        FOLLOW_UP_DATE::DATE AS FOLLOW_UP_DATE,
        MATERIALS_USED::VARCHAR(255) AS MATERIALS_USED,
        COUNTRY_CODE::VARCHAR(8) AS COUNTRY_CODE,
        current_timestamp()::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        NULL::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        CALL_DATE::DATE AS CALL_DATE,
        DETAIL_GROUP_SOURCE_ID::VARCHAR(18) AS DETAIL_GROUP_SOURCE_ID,
        USER_SOURCE_ID::VARCHAR(18) AS USER_SOURCE_ID
    FROM T1
)
SELECT *
FROM FINAL

