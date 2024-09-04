{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook = "{% if is_incremental() %}
                    DELETE FROM {{this}}
                    WHERE (CALL_DETAIL_SOURCE_ID) IN (SELECT STG.CALL_DETAIL_SOURCE_ID
                                                    FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_detail') }} STG
                                                    WHERE STG.CALL_DETAIL_SOURCE_ID = CALL_DETAIL_SOURCE_ID
                                                    AND CALL_SOURCE_ID IN (SELECT CALL_SOURCE_ID FROM {{ ref('hcposeitg_integration__itg_call') }}))
                    AND COUNTRY_CODE IN (SELECT UPPER(COUNTRY_CODE) FROM {{ ref('hcposeitg_integration__itg_call') }} WHERE  CALL_SOURCE_ID IN (SELECT CALL_SOURCE_ID FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_detail') }} )) ;
                    {% endif %}"
    )
}}

WITH sdl_hcp_osea_call_detail
AS (
    SELECT *
    FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_detail') }}
    ),
itg_CALL
AS (
    SELECT *
    FROM {{ ref('hcposeitg_integration__itg_call') }}
    ),
t1
AS (
    SELECT STG.CALL_DETAIL_SOURCE_ID,
        (
            CASE 
                WHEN UPPER(STG.IS_DELETED) = 'TRUE'
                    THEN 1
                WHEN UPPER(STG.IS_DELETED) IS NULL
                    THEN 0
                WHEN UPPER(STG.IS_DELETED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_DELETED,
        STG.CALL_DETAIL_NAME,
        STG.CREATED_DATE,
        STG.CREATED_BY_ID,
        STG.LAST_MODIFIED_DATE,
        STG.LAST_MODIFIED_BY_ID,
        STG.IS_PARENT_CALL,
        STG.CALL_SOURCE_ID,
        STG.PRODUCT_SOURCE_ID,
        STG.CALL_DETAIL_PRIORITY,
        ITG.COUNTRY_CODE,
        (
            CASE 
                WHEN UPPER(STG.IS_LOCKED) = 'TRUE'
                    THEN 1
                WHEN UPPER(STG.IS_LOCKED) IS NULL
                    THEN 0
                WHEN UPPER(STG.IS_LOCKED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_LOCKED,
        STG.DETAIL_CALL_TYPE,
        STG.CALL_DETAIL_GROUP,
        STG.PRODUCT_ID18,
        STG.CLASSIFICATION,
        STG.MY_ADOPTION_LADDER,
        STG.SIMP_ADOPTION_STYLE,
        STG.SIMP_BEHAVIORAL_STYLE,
        STG.SIMP_MARKET_SHARE,
        STG.NUMBER_OF_PATIENTS_PER_QUARTER,
        STG.NUMBER_OF_PATIENTS_PER_MONTH,
        STG.NUMBER_OF_PATIENTS_PER_WEEK,
        STG.TW_ADOPTION_LADDER,
        STG.TW_ADOPTION_STYLE,
        STG.TW_BEHAVIORAL_STYLE,
        STG.TW_MARKET_SHARE,
        current_timestamp() AS INSERTED_DATE,
        NULL AS UPDATED_DATE
    FROM sdl_hcp_OSEA_CALL_DETAIL STG
    JOIN itg_CALL ITG ON ITG.CALL_SOURCE_ID = STG.CALL_SOURCE_ID
    ),
final as
(
    select CALL_DETAIL_SOURCE_ID::VARCHAR(18) AS CALL_DETAIL_SOURCE_ID,
        IS_DELETED::VARCHAR(5) AS IS_DELETED,
        CALL_DETAIL_NAME::VARCHAR(80) AS CALL_DETAIL_NAME,
        CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
        CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
        LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
        IS_PARENT_CALL::NUMBER(18,0) AS IS_PARENT_CALL,
        CALL_SOURCE_ID::VARCHAR(18) AS CALL_SOURCE_ID,
        PRODUCT_SOURCE_ID::VARCHAR(18) AS PRODUCT_SOURCE_ID,
        CALL_DETAIL_PRIORITY::NUMBER(2,0) AS CALL_DETAIL_PRIORITY,
        COUNTRY_CODE::VARCHAR(8) AS COUNTRY_CODE,
        IS_LOCKED::NUMBER(38,0) AS IS_LOCKED,
        DETAIL_CALL_TYPE::VARCHAR(255) AS DETAIL_CALL_TYPE,
        CALL_DETAIL_GROUP::VARCHAR(18) AS CALL_DETAIL_GROUP,
        PRODUCT_ID18::VARCHAR(1300) AS PRODUCT_ID18,
        CLASSIFICATION::VARCHAR(40) AS CLASSIFICATION,
        MY_ADOPTION_LADDER::VARCHAR(255) AS MY_ADOPTION_LADDER,
        SIMP_ADOPTION_STYLE::VARCHAR(255) AS SIMP_ADOPTION_STYLE,
        SIMP_BEHAVIORAL_STYLE::VARCHAR(255) AS SIMP_BEHAVIORAL_STYLE,
        SIMP_MARKET_SHARE::NUMBER(18,1) AS SIMP_MARKET_SHARE,
        NUMBER_OF_PATIENTS_PER_QUARTER::NUMBER(18,1) AS NUMBER_OF_PATIENTS_PER_QUARTER,
        NUMBER_OF_PATIENTS_PER_MONTH::NUMBER(18,1) AS NUMBER_OF_PATIENTS_PER_MONTH,
        NUMBER_OF_PATIENTS_PER_WEEK::NUMBER(18,1) AS NUMBER_OF_PATIENTS_PER_WEEK,
        TW_ADOPTION_LADDER::VARCHAR(255) AS TW_ADOPTION_LADDER,
        TW_ADOPTION_STYLE::VARCHAR(255) AS TW_ADOPTION_STYLE,
        TW_BEHAVIORAL_STYLE::VARCHAR(255) AS TW_BEHAVIORAL_STYLE,
        TW_MARKET_SHARE::NUMBER(18,1) AS TW_MARKET_SHARE,
        INSERTED_DATE::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        UPDATED_DATE::TIMESTAMP_NTZ(9) AS UPDATED_DATE
    from t1
)
SELECT *
FROM FINAL
