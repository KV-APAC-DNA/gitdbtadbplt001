{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} WHERE HCO_KEY IN (SELECT HCO_KEY FROM {{ source('snapinditg_integration','itg_hcp360_veeva_account_hco') }} as ITG_HCO
        WHERE ITG_HCO.HCO_KEY = {{ this }}.HCO_KEY);
        DELETE FROM {{this}} WHERE HCO_KEY = 'Not Applicable';
        {% endif %}"
    )
}}
with itg_hcp360_veeva_account_hco as
(
    select * from {{ source('snapinditg_integration','itg_hcp360_veeva_account_hco') }}
),
itg_hcp360_veeva_recordtype as
(
    select * from {{ source('snapinditg_integration','itg_hcp360_veeva_recordtype') }}
),
itg_hcp360_veeva_address as
(
    select * from {{ source('snapinditg_integration','itg_hcp360_veeva_address') }}
),
final as(
    SELECT DISTINCT 
      HCO_KEY,
      ITG_HCO.COUNTRY_CODE,
      ITG_HCO.ACCOUNT_SOURCE_ID as HCO_SOURCE_ID,
      NVL(ITG_HCO.LAST_MODIFIED_BY_ID, 'Not Applicable') AS MODIFY_ID,
      ITG_HCO.LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) as MODIFY_DT,
      'Not Applicable'::VARCHAR(123) as HCO_BUSINESS_ID,
      NVL(ITG_HCO.INACTIVE, 0)::NUMBER(18,0) as INACTIVE_FLAG,
      'Not Applicable'::VARCHAR(255) as INACTIVE_REASON,
      ACCOUNT_NAME::VARCHAR(1300) as HCO_NAME,
      HCO_TYPE,
      --NVL(TARGET_VALUE,HCO_TYPE) AS HCO_TYPE_ENGLISH_NAME,
      HCO_SECTOR as SECTOR,
      MOBILE_PHONE as PHONE_NUMBER,
      FAX_NUMBER,
      WEBSITE,
      TERRITORY_NAME,
      HCO_NAME as FORMATTED_NAME,
      REMARKS,
      IS_EXCLUDED_FROM_REALIGN as EXCLUDE_FROM_TERRITORY_ASSIGNMENT_RULES,
      BEDS::NUMBER(8,0) as BEDS,
      TOTAL_MDS_DOS,
      DEPARTMENTS,
      SFE_APPROVED as SFE_APPROVED_FLAG,
      --BUSINESS_DESCRIPTION,
      HCC_ACCOUNT_APPROVED,
      TOTAL_PHYSICIANS_ENROLLED,
      TOTAL_PHARMACISTS,
      --IS_EXTERNAL_ID_NUMBER,
      ITG_REC.RECORD_TYPE_NAME,
      NVL(ITG_HCO.EXTERNAL_ID, 'Not Applicable') AS HCO_EXTERNAL_ID,
      PARENT_HCO_KEY as PARENT_HCO_KEY,
      PARENT_HCO_NAME,
      ITG_HCO.IS_DELETED as DELETED_FLAG,
      ADDRESS_NAME as ADDRESS_LINE_1,
      ADDRESS_LINE2_NAME as ADDRESS_LINE_2,
      SUBURB_TOWN,
      CITY,
      STATE,
      ZIP as POSTCODE,
      BRICK,
      MAP,
      NVL(ADDRESS_SOURCE_ID, 'Not Applicable') AS HCO_ADDRESS_SOURCE_ID,
      APPT_REQUIRED as APPT_REQUIRED_FLAG,
      ITG_ADD.EXTERNAL_ID::VARCHAR(123) as ADDRESS_EXTERNAL_ID,
      PHONE::VARCHAR(43) as ADDRESS_PHONE,
      FAX::VARCHAR(43) as ADDRESS_FAX,
      ITG_ADD.IS_PRIMARY as PRIMARY_FLAG,
      ITG_ADD.INACTIVE as ADDRESS_INACTIVE_FLAG,
      CONTROLLING_ADDRESS,
      VEEVA_AUTOGEN_ID::VARCHAR(33) as VEEVA_AUTOGEN_ID,
      CUSTOMER_CODE,
      current_timestamp()::timestamp_ntz(9) as crt_dttm,
      current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM itg_hcp360_veeva_account_hco ITG_HCO
    JOIN itg_hcp360_veeva_recordtype ITG_REC ON ITG_HCO.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
      AND ITG_HCO.COUNTRY_CODE = ITG_REC.COUNTRY_CODE
    LEFT OUTER JOIN (
      SELECT *
      FROM itg_hcp360_veeva_address
      WHERE address_source_id IN (
          SELECT max(address_source_id)
          FROM itg_hcp360_veeva_address
          WHERE is_primary = 1
          GROUP BY account_source_id
          )
        OR address_source_id IN (
          SELECT max(address_source_id)
          FROM itg_hcp360_veeva_address
          WHERE is_primary = 0
            AND account_source_id NOT IN (
              SELECT account_source_id
              FROM itg_hcp360_veeva_address
              WHERE is_primary = 1
              )
          GROUP BY account_source_id
          )
      ) AS ITG_ADD ON ITG_ADD.ACCOUNT_SOURCE_ID = decode(ITG_HCO.PRIMARY_PARENT_SOURCE_ID, NULL, ITG_HCO.ACCOUNT_SOURCE_ID, ' ', ITG_HCO.ACCOUNT_SOURCE_ID, ITG_HCO.PRIMARY_PARENT_SOURCE_ID)
      AND ITG_HCO.COUNTRY_CODE = ITG_ADD.COUNTRY_CODE 
    union all
    SELECT
        'Not Applicable' as HCO_KEY,
        'ZZ' as COUNTRY_CODE,
        'Not Applicable' as HCO_SOURCE_ID,
        'Not Applicable' as MODIFY_ID,
        current_timestamp()::timestamp_ntz(9) as MODIFY_DT,
        'Not Applicable'::VARCHAR(123) as HCO_BUSINESS_ID,
        0::NUMBER(18,0) as INACTIVE_FLAG,
        'Not Applicable'::VARCHAR(255) as INACTIVE_REASON,
        'Not Applicable'::VARCHAR(1300) as HCO_NAME,
        'Not Applicable' as HCO_TYPE,
        --'Not Applicable' as HCO_TYPE_ENGLISH_NAME,
        'Not Applicable' as SECTOR,
        'Not Applicable' as PHONE_NUMBER,
        'Not Applicable' as FAX_NUMBER,
        'Not Applicable' as WEBSITE,
        'Not Applicable' as TERRITORY_NAME,
        'Not Applicable' as FORMATTED_NAME,
        'Not Applicable' as REMARKS,
        0 as EXCLUDE_FROM_TERRITORY_ASSIGNMENT_RULES,
        0::NUMBER(8,0) as BEDS,
        0 as TOTAL_MDS_DOS,
        0 as DEPARTMENTS,
        0 as SFE_APPROVED_FLAG,
        --'Not Applicable' as BUSINESS_DESCRIPTION,
        0 as HCC_ACCOUNT_APPROVED,
        0 as TOTAL_PHYSICIANS_ENROLLED,
        0 as TOTAL_PHARMACISTS,
        --0 as IS_EXTERNAL_ID_NUMBER,
        'Not Applicable' as RECORD_TYPE_NAME,
        'Not Applicable' as HCO_EXTERNAL_ID,
        'Not Applicable' as PARENT_HCO_KEY,
        'Not Applicable' as PARENT_HCO_NAME,
        0 as DELETED_FLAG,
        'Not Applicable' as ADDRESS_LINE_1,
        'Not Applicable' as ADDRESS_LINE_2,
        'Not Applicable' as SUBURB_TOWN,
        'Not Applicable' as CITY,
        'Not Applicable' as STATE,
        'Not Applicable' as POSTCODE,
        'Not Applicable' as BRICK,
        'Not Applicable' as MAP,
        'Not Applicable' as HCO_ADDRESS_SOURCE_ID,
        0 as APPT_REQUIRED_FLAG,
        'Not Applicable'::VARCHAR(123) as ADDRESS_EXTERNAL_ID,
        'Not Applicable'::VARCHAR(43) as ADDRESS_PHONE,
        'Not Applicable'::VARCHAR(43) as ADDRESS_FAX,
        0 as PRIMARY_FLAG,
        0 as ADDRESS_INACTIVE_FLAG,
        'Not Applicable' as CONTROLLING_ADDRESS,
        'Not Applicable'::VARCHAR(33) as VEEVA_AUTOGEN_ID,
        'Not Applicable' as CUSTOMER_CODE,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) as UPDT_DTTM
    )
select * from final