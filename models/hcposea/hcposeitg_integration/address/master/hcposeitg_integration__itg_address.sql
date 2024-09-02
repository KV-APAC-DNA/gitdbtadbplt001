{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="DELETE FROM {{this}} 
                    WHERE ADDRESS_SOURCE_ID IN (SELECT ADDRESS_SOURCE_ID
                                                FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_address') }} STG_ADDRESS
                                                WHERE STG_ADDRESS.ADDRESS_SOURCE_ID = ADDRESS_SOURCE_ID)
                    AND   COUNTRY_CODE IN (SELECT DISTINCT UPPER(COUNTRY_CODE)
                                        FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_address') }});"
                                                )  }}




WITH sdl_hcp_osea_address
AS (
    SELECT *
    FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_address') }}
    ),
t1
AS (
    SELECT ADDRESS_SOURCE_ID,
        (
            CASE 
                WHEN UPPER(IS_DELETED) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_DELETED) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_DELETED,
        ADDRESS_NAME,
        RECORD_TYPE_SOURCE_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        ACCOUNT_SOURCE_ID,
        ADDRESS_LINE2_NAME,
        CITY,
        EXTERNAL_ID,
        PHONE,
        FAX,
        MAP,
        (
            CASE 
                WHEN UPPER(IS_PRIMARY) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_PRIMARY) IS NULL
                    THEN 0
                WHEN UPPER(IS_PRIMARY) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_PRIMARY,
        (
            CASE 
                WHEN UPPER(APPT_REQUIRED) = 'TRUE'
                    THEN 1
                WHEN UPPER(APPT_REQUIRED) IS NULL
                    THEN 0
                WHEN UPPER(APPT_REQUIRED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS APPT_REQUIRED,
        (
            CASE 
                WHEN UPPER(INACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(INACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS INACTIVE,
        nvl(UPPER(COUNTRY_CODE), SUBSTRING(STATE, 1, 2)) AS COUNTRY_CODE,
        Latitude,
        ZIP,
        BRICK,
        STATE,
        Longitude,
        CONTROLLING_ADDRESS,
        SUBURB_TOWN,
        VEEVA_AUTOGEN_ID,
        SYSDATE() as inserted_date,
        NULL as updated_date,
        (
            CASE 
                WHEN UPPER(IS_LOCKED) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_LOCKED) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_LOCKED
    FROM sdl_hcp_OSEA_ADDRESS
    WHERE COUNTRY_CODE IS NOT NULL
        OR COUNTRY_CODE != ''
        OR COUNTRY_CODE != ' '
    ),
final
AS (
    SELECT ADDRESS_SOURCE_ID::VARCHAR(18) AS ADDRESS_SOURCE_ID,
        IS_DELETED::NUMBER(38, 0) AS IS_DELETED,
        ADDRESS_NAME::VARCHAR(85) AS ADDRESS_NAME,
        RECORD_TYPE_SOURCE_ID::VARCHAR(18) AS RECORD_TYPE_SOURCE_ID,
        CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
        CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
        LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
        ACCOUNT_SOURCE_ID::VARCHAR(300) AS ACCOUNT_SOURCE_ID,
        ADDRESS_LINE2_NAME::VARCHAR(300) AS ADDRESS_LINE2_NAME,
        CITY::VARCHAR(40) AS CITY,
        EXTERNAL_ID::VARCHAR(120) AS EXTERNAL_ID,
        PHONE::VARCHAR(40) AS PHONE,
        FAX::VARCHAR(40) AS FAX,
        MAP::VARCHAR(1300) AS MAP,
        IS_PRIMARY::NUMBER(38, 0) AS IS_PRIMARY,
        APPT_REQUIRED::NUMBER(38, 0) AS APPT_REQUIRED,
        INACTIVE::NUMBER(38, 0) AS INACTIVE,
        COUNTRY_CODE::VARCHAR(255) AS COUNTRY_CODE,
        LATITUDE::NUMBER(15, 8) AS LATITUDE,
        ZIP::VARCHAR(20) AS ZIP,
        BRICK::VARCHAR(80) AS BRICK,
        STATE::VARCHAR(255) AS STATE,
        LONGITUDE::NUMBER(15, 8) AS LONGITUDE,
        CONTROLLING_ADDRESS::VARCHAR(18) AS CONTROLLING_ADDRESS,
        SUBURB_TOWN::VARCHAR(50) AS SUBURB_TOWN,
        VEEVA_AUTOGEN_ID::VARCHAR(30) AS VEEVA_AUTOGEN_ID,
        INSERTED_DATE::TIMESTAMP_NTZ(9) AS INSERTED_DATE,
        UPDATED_DATE::TIMESTAMP_NTZ(9) AS UPDATED_DATE,
        IS_LOCKED::VARCHAR(5) AS IS_LOCKED,
    FROM t1
    )
SELECT *
FROM final