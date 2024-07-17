{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        DELETE FROM {{this}} WHERE (HCP_KEY) IN (SELECT HCP_KEY FROM {{ source('snapinditg_integration', 'itg_hcp360_veeva_account_hcp') }} as ITG_HCP WHERE ITG_HCP.HCP_KEY = {{ this }}.HCP_KEY);
        DELETE FROM {{this}} WHERE HCP_KEY = 'Not Applicable';
        {% endif %}"
    )
}}
with itg_hcp360_veeva_account_hcp as
(
    select * from {{ source('snapinditg_integration', 'itg_hcp360_veeva_account_hcp') }}
),
itg_hcp360_veeva_recordtype as
(
    select * from {{ source('snapinditg_integration', 'itg_hcp360_veeva_recordtype') }}
),
itg_hcp360_veeva_account_hco as
(
    select * from {{ source('snapinditg_integration', 'itg_hcp360_veeva_account_hco') }}
),
EDW_HCP360_HCP_MASTER_KEY_BY_BRAND as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_hcp_master_key_by_brand') }}   
),
final as(
    SELECT DISTINCT
      ITG_HCP.HCP_KEY,
      ITG_HCP.COUNTRY_CODE,
      ITG_HCP.ACCOUNT_SOURCE_ID as HCP_SOURCE_ID,
      ITG_HCP.LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) as MODIFY_DT,
      ITG_HCP.LAST_MODIFIED_BY_ID as MODIFY_ID,
      NVL(ITG_HCP.EXTERNAL_ID, 'Not Applicable')::VARCHAR(30) as HCP_BUSINESS_ID,
      ITG_HCP.PROFESSIONAL_TYPE AS HCP_TYPE,
      -- NVL(HCP_TYPE_ENG.TARGET_VALUE,ITG_HCP.HCP_TYPE) AS HCP_TYPE_ENGLISH,
      --NVL(HCP_TYPE_ENG.TARGET_VALUE,ITG_HCP.PROFESSIONAL_TYPE) AS HCP_TYPE_ENGLISH,
      ITG_HCP.INACTIVE as INACTIVE_FLAG,
      (
        CASE
          WHEN UPPER(ITG_HCP.INACTIVE) = 1
            THEN current_timestamp()
          WHEN UPPER(ITG_HCP.INACTIVE) IS NULL
            THEN NULL
          WHEN UPPER(ITG_HCP.INACTIVE) = ' '
            THEN NULL
          ELSE NULL
          END
        )::date AS INACTIVE_DATE,
      'Not Applicable'::VARCHAR(255) AS INACTIVE_REASON,
      ITG_HCP.HCP_NAME,
      ITG_HCP.HCP_ENGLISH_NAME,
      ITG_HCP.HCP_NAME as HCP_DISPLAY_NAME,
      NULL::VARCHAR(255) as GENDER,
      --ITG_HCP.BIRTH_DAY,
      ITG_HCP.SPECIALTY_1 as SPECIALITY_1_TYPE,
      --NVL(SPEC1_ENG.TARGET_VALUE,ITG_HCP.SPECIALTY_1) AS SPECIALITY_1_TYPE_ENGLISH,
      ITG_HCP.POSITION as POSITION_NAME,
      --NVL(HCP_POS_ENG.TARGET_VALUE,ITG_HCP.POSITION) AS POSITION_NAME_ENGLISH,
      ITG_HCP.TERRITORY_NAME,
      ITG_HCP.IS_KOL as KOL_FLAG,
      'Not Applicable'::VARCHAR(255) AS LICENSE_CD,
      ITG_HCP.DIRECT_LINE as DIRECT_LINE_NUMBER,
      ITG_HCP.DIRECT_FAX as DIRECT_FAX_NUMBER,
      ITG_HCP.PERSON_EMAIL::VARCHAR(255) as EMAIL_ID,
      ITG_HCP.MOBILE_PHONE as MOBILE_NBR,
      ITG_HCP.DO_NOT_CALL_IND as DO_NOT_CALL_FLAG,
      ITG_HCP.EMAIL_OPT_OUT_IND as EMAIL_OPT_OUT_FLAG,
      ITG_HCO.HCO_KEY as PARENT_HCO_KEY,
      ITG_HCO.HCO_NAME as PARENT_HCO_NAME,
      ITG_REC.RECORD_TYPE_NAME,
      NVL(ITG_HCP.EXTERNAL_ID, 'Not Applicable') AS HCP_EXTERNAL_ID,
      ITG_HCP.REMARKS,
      ITG_HCP.IS_DELETED as DELETED_FLAG,
      ITG_HCP.PREFERRED_NAME::VARCHAR(255) as PREFERRED_NAME,
      ITG_HCP.PROFESSIONAL_TYPE,
      ITG_HCP.GO_CLASSIFICATION,
      ITG_HCP.HCC_ACCOUNT_APPROVED as HCC_ACCOUNT_APPROVED_FLAG,
      ITG_HCP.SALUTATION,
      ITG_HCP.IS_EXCLUDED_FROM_REALIGN as EXCLUDE_FROM_TERRITORY_ASSIGNMENT_RULES_FLAG,
      ITG_HCP.SFE_APPROVED as SFE_APPROVED_FLAG,
      ITG_HCP.PHYSICIAN_PRESCRIBING_BEHAVIOR::VARCHAR(255) AS PHYSICIAN_PRESCRIBING_BEHAVIOR,
      ITG_HCP.PHYSICIAN_BEHAVIORAL_STYLE,
      --ITG_HCP.IS_EXTERNAL_ID_NUMBER,
      ITG_HCP.CUSTOMER_VALUE_SEGMENTATION,
      ITG_HCP.ABILITY_TO_INFLUENCE_PEERS,
      ITG_HCP.PRACTICE_SIZE,
      ITG_HCP.PATIENT_TYPE,
      ITG_HCP.PATIENT_MEDICAL_CONDITION,
      ITG_HCP.MD_CUSTOMER_SEGMENTATION,
      ITG_HCP.YEARS_OF_EXPERIENCE,
      ITG_HCP.MD_INNOVATION,
      ITG_HCP.MD_NUMBER_OF_PROCEDURES,
      ITG_HCP.MD_TYPES_OF_PROCEDURE,
      ITG_HCP.MD_TOTAL_HIP_REPLACEMENT,
      ITG_HCP.MD_TOTAL_KNEE_REPLACEMENT,
      ITG_HCP.MD_SPINE,
      ITG_HCP.MD_TRAUMA,
      ITG_HCP.MD_COLLORECTAL,
      ITG_HCP.MD_HEPATOBILIARY,
      ITG_HCP.MD_CHOLECYSTECTOMY,
      ITG_HCP.MD_TOTAL_HYSTERECTOMY,
      ITG_HCP.MD_MYOMECTOMY,
      ITG_HCP.MD_C_SECTION,
      ITG_HCP.MD_NORMAL_DELIVERY,
      ITG_HCP.MD_CABG,
      ITG_HCP.MD_VALVE_REPAIR,
      ITG_HCP.MD_ABDOMINAL,
      ITG_HCP.MD_BREAST_RECONSTRUCTION,
      ITG_HCP.MD_ORAL_CRANIAL_MAXILOFACIAL,
      current_timestamp()::timestamp_ntz(9) as crt_dttm,
      current_timestamp()::timestamp_ntz(9) as updt_dttm,
      ITG_HCP.PRIMARY_TA::VARCHAR(255) as PRIMARY_TA,
      --ITG_HCP.SECONDARY_TA
      MKEY.MASTER_HCP_KEY as HCP_MASTER_KEY,
      ITG_HCP.CREATED_DATE::TIMESTAMP_NTZ(9) as HCP_CREATED_DATE
    FROM itg_hcp360_veeva_account_hcp ITG_HCP
    JOIN itg_hcp360_veeva_recordtype ITG_REC ON ITG_HCP.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
      AND ITG_HCP.COUNTRY_CODE = ITG_REC.COUNTRY_CODE
    LEFT OUTER JOIN itg_hcp360_veeva_account_hco ITG_HCO ON ITG_HCO.ACCOUNT_SOURCE_ID = ITG_HCP.PRIMARY_PARENT_SOURCE_ID
      AND ITG_HCO.COUNTRY_CODE = ITG_HCP.COUNTRY_CODE
    LEFT OUTER JOIN EDW_HCP360_HCP_MASTER_KEY_BY_BRAND MKEY ON MKEY.ACCOUNT_SOURCE_ID = ITG_HCP.ACCOUNT_SOURCE_ID
    union all
    select
        'Not Applicable' as HCP_KEY,
        'ZZ' as COUNTRY_CODE,
        'NA' as HCP_SOURCE_ID,
        current_timestamp()::timestamp_ntz(9) as MODIFY_DT,
        'Not Applicable' as MODIFY_ID,
        'Not Applicable':: VARCHAR(30) as HCP_BUSINESS_ID,
        'Not Applicable' as HCP_TYPE,
        --'Not Applicable' as HCP_TYPE_ENGLISH,
        0 as INACTIVE_FLAG,
        current_date()::date as INACTIVE_DATE,
        'NOT APPLICABLE'::VARCHAR(255) AS INACTIVE_REASON,
        'Not Applicable' as HCP_NAME,
        'Not Applicable' as HCP_ENGLISH_NAME,
        'Not Applicable' as HCP_DISPLAY_NAME,
        'Not Applicable'::VARCHAR(255) as GENDER,
        --SYSDATE as BIRTH_DAY,
        'Not Applicable' as SPECIALITY_1_TYPE,
        --'Not Applicable' as SPECIALITY_1_TYPE_ENGLISH,
        'Not Applicable' as POSITION_NAME,
        --'Not Applicable' as POSITION_NAME_ENGLISH,
        'Not Applicable' as TERRITORY_NAME,
        0 as KOL_FLAG,
        'Not Applicable'::VARCHAR(255) AS LICENSE_CD,
        'Not Applicable' as DIRECT_LINE_NUMBER,
        'Not Applicable' as DIRECT_FAX_NUMBER,
        'Not Applicable'::VARCHAR(255) as EMAIL_ID,
        'Not Applicable' as MOBILE_NBR,
        0 as DO_NOT_CALL_FLAG,
        0 as EMAIL_OPT_OUT_FLAG,
        'Not Applicable' as PARENT_HCO_KEY,
        'Not Applicable' as PARENT_HCO_NAME,
        'Not Applicable' as RECORD_TYPE_NAME,
        'Not Applicable' as HCP_EXTERNAL_ID,
        'Not Applicable' as REMARKS,
        0 as DELETED_FLAG,
        'Not Applicable'::VARCHAR(255) as PREFERRED_NAME,
        'Not Applicable' as PROFESSIONAL_TYPE,
        'Not Applicable' as GO_CLASSIFICATION,
        0 as HCC_ACCOUNT_APPROVED_FLAG,
        'Not Applicable' as SALUTATION,
        0 AS EXCLUDE_FROM_TERRITORY_ASSIGNMENT_RULES_FLAG,
        0 AS SFE_APPROVED_FLAG,
        0::VARCHAR(255) AS PHYSICIAN_PRESCRIBING_BEHAVIOR,
        'Not Applicable' AS PHYSICIAN_BEHAVIORAL_STYLE,
        --0 AS IS_EXTERNAL_ID_NUMBER,
        'Not Applicable' AS CUSTOMER_VALUE_SEGMENTATION,
        'Not Applicable' AS ABILITY_TO_INFLUENCE_PEERS,
        'Not Applicable' AS PRACTICE_SIZE,
        'Not Applicable' AS PATIENT_TYPE,
        'Not Applicable' AS PATIENT_MEDICAL_CONDITION,
        'Not Applicable' AS MD_CUSTOMER_SEGMENTATION,
        'Not Applicable' AS YEARS_OF_EXPERIENCE,
        'Not Applicable' AS MD_INNOVATION,
        'Not Applicable' AS MD_NUMBER_OF_PROCEDURES,
        'Not Applicable' AS MD_TYPES_OF_PROCEDURE,
        0 AS MD_TOTAL_HIP_REPLACEMENT,
        0 AS MD_TOTAL_KNEE_REPLACEMENT,
        0 AS MD_SPINE,
        0 AS MD_TRAUMA,
        0 AS MD_COLLORECTAL,
        0 AS MD_HEPATOBILIARY,
        0 AS MD_CHOLECYSTECTOMY,
        0 AS MD_TOTAL_HYSTERECTOMY,
        0 AS MD_MYOMECTOMY,
        0 AS MD_C_SECTION,
        0 AS MD_NORMAL_DELIVERY,
        0 AS MD_CABG,
        0 AS MD_VALVE_REPAIR,
        0 AS MD_ABDOMINAL,
        0 AS MD_BREAST_RECONSTRUCTION,
        0 AS MD_ORAL_CRANIAL_MAXILOFACIAL,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM,
        current_timestamp()::timestamp_ntz(9) as UPDT_DTTM,
        'Not Applicable'::VARCHAR(255) as PRIMARY_TA,
        'Not Applicable' AS HCP_MASTER_KEY,
        current_timestamp()::timestamp_ntz(9) as HCP_CREATED_DATE
    )
select * from final