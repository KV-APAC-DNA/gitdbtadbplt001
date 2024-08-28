{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="delete
                    from {{this}}
                    where (account_source_id) in (
                            select account_source_id
                            from {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hcp') }} stg_hcp
                            where stg_hcp.account_source_id = account_source_id
                            )
                        and country_code in (
                            select distinct upper(country_code)
                            from {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hcp') }}
                            );"
    )
}}

with sdl_hcp_osea_account_hcp as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_account_hcp') }}
)
t1
as (
    select md5(upper(country_code) || account_source_id) as hcp_key,
        account_source_id,
        (
            case 
                WHEN UPPER(IS_DELETED) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_DELETED) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS IS_DELETED,
        HCP_NAME,
        LAST_NAME,
        FIRST_NAME,
        SALUTATION,
        RECORD_TYPE_SOURCE_ID,
        OWNER_ID,
        CREATED_DATE,
        CREATED_BY_ID,
        LAST_MODIFIED_DATE,
        LAST_MODIFIED_BY_ID,
        (
            CASE 
                WHEN UPPER(IS_EXCLUDED_FROM_REALIGN) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_EXCLUDED_FROM_REALIGN) IS NULL
                    THEN 0
                WHEN UPPER(IS_EXCLUDED_FROM_REALIGN) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_EXCLUDED_FROM_REALIGN,
        (
            CASE 
                WHEN UPPER(IS_PERSON_ACCOUNT) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_PERSON_ACCOUNT) IS NULL
                    THEN 0
                WHEN UPPER(IS_PERSON_ACCOUNT) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_PERSON_ACCOUNT,
        PERSON_MOBILE_PHONE,
        PERSON_EMAIL,
        BIRTH_DAY,
        (
            CASE 
                WHEN UPPER(PERSON_HAS_OPTED_OUT_OF_EMAIL) = 'TRUE'
                    THEN 1
                WHEN UPPER(PERSON_HAS_OPTED_OUT_OF_EMAIL) IS NULL
                    THEN 0
                WHEN UPPER(PERSON_HAS_OPTED_OUT_OF_EMAIL) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS EMAIL_OPT_OUT_IND,
        (
            CASE 
                WHEN UPPER(PERSON_DO_NOT_CALL) = 'TRUE'
                    THEN 1
                WHEN UPPER(PERSON_DO_NOT_CALL) IS NULL
                    THEN 0
                WHEN UPPER(PERSON_DO_NOT_CALL) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS DO_NOT_CALL_IND,
        EXTERNAL_ID,
        TERRITORY_NAME,
        SPECIALTY_1,
        GENDER,
        PREFERRED_NAME,
        PRIMARY_PARENT_SOURCE_ID,
        PROFESSIONAL_TYPE,
        HCP_ENGLISH_NAME,
        POSITION,
        DIRECT_LINE,
        DIRECT_FAX,
        (
            CASE 
                WHEN UPPER(SFE_APPROVED) = 'TRUE'
                    THEN 1
                WHEN UPPER(SFE_APPROVED) IS NULL
                    THEN 0
                WHEN UPPER(SFE_APPROVED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS SFE_APPROVED,
        UPPER(COUNTRY_CODE) as COUNTRY_CODE,
        GO_CLASSIFICATION,
        (
            CASE 
                WHEN UPPER(HCC_ACCOUNT_APPROVED) = 'TRUE'
                    THEN 1
                WHEN UPPER(HCC_ACCOUNT_APPROVED) IS NULL
                    THEN 0
                WHEN UPPER(HCC_ACCOUNT_APPROVED) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS HCC_ACCOUNT_APPROVED,
        (
            CASE 
                WHEN UPPER(IS_KOL) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_KOL) IS NULL
                    THEN 0
                WHEN UPPER(IS_KOL) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_KOL,
        (
            CASE 
                WHEN UPPER(INACTIVE) = 'TRUE'
                    THEN 1
                WHEN UPPER(INACTIVE) = 'FALSE'
                    THEN 0
                ELSE 0
                END
            ) AS INACTIVE,
        PHYSICIAN_PRESCRIBING_BEHAVIOR,
        PHYSICIAN_BEHAVIORAL_STYLE,
        (
            CASE 
                WHEN UPPER(IS_EXTERNAL_ID_NUMBER) = 'TRUE'
                    THEN 1
                WHEN UPPER(IS_EXTERNAL_ID_NUMBER) IS NULL
                    THEN 0
                WHEN UPPER(IS_EXTERNAL_ID_NUMBER) = ' '
                    THEN 0
                ELSE 0
                END
            ) AS IS_EXTERNAL_ID_NUMBER,
        CUSTOMER_VALUE_SEGMENTATION,
        ABILITY_TO_INFLUENCE_PEERS,
        PRACTICE_SIZE,
        PATIENT_TYPE,
        PATIENT_MEDICAL_CONDITION,
        MD_CUSTOMER_SEGMENTATION,
        YEARS_OF_EXPERIENCE,
        MD_INNOVATION,
        MD_NUMBER_OF_PROCEDURES,
        MD_TYPES_OF_PROCEDURE,
        MD_TOTAL_HIP_REPLACEMENT,
        MD_TOTAL_KNEE_REPLACEMENT,
        MD_SPINE,
        MD_TRAUMA,
        MD_COLLORECTAL,
        MD_HEPATOBILIARY,
        MD_CHOLECYSTECTOMY,
        MD_TOTAL_HYSTERECTOMY,
        MD_MYOMECTOMY,
        MD_C_SECTION,
        MD_NORMAL_DELIVERY,
        MD_CABG,
        MD_VALVE_REPAIR,
        MD_ABDOMINAL,
        MD_BREAST_RECONSTRUCTION,
        MD_ORAL_CRANIAL_MAXILOFACIAL,
        HCP_TYPE,
        REMARKS,
        ACCOUNT_CLASSIFICATION,
        CUSTOMER_CODE_2,
        SYSDATE() as inserted_date,
        NULL as updated_date,
        PRIMARY_TA,
        SECONDARY_TA,
        jj_external_id,
        Specialty_2,
        Sub_Specialty,
        sea_account_classification,
        jj_email_1,
        jj_email_2
    FROM sdl_hcp_OSEA_ACCOUNT_HCP
    ),
final AS
(
    select HCP_KEY::VARCHAR(32) as HCP_KEY,
	ACCOUNT_SOURCE_ID::VARCHAR(18) as ACCOUNT_SOURCE_ID,
	IS_DELETED::NUMBER(38,0) as IS_DELETED,
	HCP_NAME::VARCHAR(255) as HCP_NAME,
	LAST_NAME::VARCHAR(80) as LAST_NAME,
	FIRST_NAME::VARCHAR(40) as FIRST_NAME,
	SALUTATION::VARCHAR(120) as SALUTATION,
	RECORD_TYPE_SOURCE_ID::VARCHAR(18) as RECORD_TYPE_SOURCE_ID,
	OWNER_ID::VARCHAR(18) as OWNER_ID,
	CREATED_DATE::TIMESTAMP_NTZ(9) as CREATED_DATE,
	CREATED_BY_ID::VARCHAR(18) as CREATED_BY_ID,
	LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) as LAST_MODIFIED_DATE,
	LAST_MODIFIED_BY_ID::VARCHAR(18) as LAST_MODIFIED_BY_ID,
	IS_EXCLUDED_FROM_REALIGN::NUMBER(38,0) as IS_EXCLUDED_FROM_REALIGN,
	IS_PERSON_ACCOUNT::NUMBER(38,0) as IS_PERSON_ACCOUNT,
	PERSON_MOBILE_PHONE::VARCHAR(40) as MOBILE_NBR,
	PERSON_EMAIL::VARCHAR(80) as PERSON_EMAIL,
	BIRTH_DAY::TIMESTAMP_NTZ(9) as BIRTH_DAY,
	EMAIL_OPT_OUT_IND::NUMBER(38,0) as EMAIL_OPT_OUT_IND,
	DO_NOT_CALL_IND::NUMBER(38,0) as DO_NOT_CALL_IND,
	EXTERNAL_ID::VARCHAR(120) as EXTERNAL_ID,
	TERRITORY_NAME::VARCHAR(255) as TERRITORY_NAME,
	SPECIALTY_1::VARCHAR(255) as SPECIALTY_1,
	GENDER::VARCHAR(255) as GENDER,
	PREFERRED_NAME::VARCHAR(12) as PREFERRED_NAME,
	PRIMARY_PARENT_SOURCE_ID::VARCHAR(18) as PRIMARY_PARENT_SOURCE_ID,
	PROFESSIONAL_TYPE::VARCHAR(255) as PROFESSIONAL_TYPE,
	HCP_ENGLISH_NAME::VARCHAR(800) as HCP_ENGLISH_NAME,
	POSITION::VARCHAR(255) as POSITION,
	DIRECT_LINE::VARCHAR(40) as DIRECT_LINE,
	DIRECT_FAX::VARCHAR(40) as DIRECT_FAX,
	SFE_APPROVED::NUMBER(38,0) as SFE_APPROVED,
	COUNTRY_CODE::VARCHAR(8) as COUNTRY_CODE,
	GO_CLASSIFICATION::VARCHAR(255) as GO_CLASSIFICATION,
	HCC_ACCOUNT_APPROVED::NUMBER(38,0) as HCC_ACCOUNT_APPROVED,
	IS_KOL::NUMBER(38,0) as IS_KOL,
	INACTIVE::NUMBER(38,0) as INACTIVE,
	PHYSICIAN_PRESCRIBING_BEHAVIOR::VARCHAR(255) as PHYSICIAN_PRESCRIBING_BEHAVIOR,
	PHYSICIAN_BEHAVIORAL_STYLE::VARCHAR(255) as PHYSICIAN_BEHAVIORAL_STYLE,
	IS_EXTERNAL_ID_NUMBER::VARCHAR(5) as IS_EXTERNAL_ID_NUMBER,
	CUSTOMER_VALUE_SEGMENTATION::VARCHAR(255) as CUSTOMER_VALUE_SEGMENTATION,
	ABILITY_TO_INFLUENCE_PEERS::VARCHAR(255) as ABILITY_TO_INFLUENCE_PEERS,
	PRACTICE_SIZE::VARCHAR(255) as PRACTICE_SIZE,
	PATIENT_TYPE::VARCHAR(255) as PATIENT_TYPE,
	PATIENT_MEDICAL_CONDITION::VARCHAR(255) as PATIENT_MEDICAL_CONDITION,
	MD_CUSTOMER_SEGMENTATION::VARCHAR(255) as MD_CUSTOMER_SEGMENTATION,
	YEARS_OF_EXPERIENCE::VARCHAR(255) as YEARS_OF_EXPERIENCE,
	MD_INNOVATION::VARCHAR(255) as MD_INNOVATION,
	MD_NUMBER_OF_PROCEDURES::VARCHAR(255) as MD_NUMBER_OF_PROCEDURES,
	MD_TYPES_OF_PROCEDURE::VARCHAR(255) as MD_TYPES_OF_PROCEDURE,
	MD_TOTAL_HIP_REPLACEMENT::NUMBER(18,0) as MD_TOTAL_HIP_REPLACEMENT,
	MD_TOTAL_KNEE_REPLACEMENT::NUMBER(18,0) as MD_TOTAL_KNEE_REPLACEMENT,
	MD_SPINE::NUMBER(18,0) as MD_SPINE,
	MD_TRAUMA::NUMBER(18,0) as MD_TRAUMA,
	MD_COLLORECTAL::NUMBER(18,0) as MD_COLLORECTAL,
	MD_HEPATOBILIARY::NUMBER(18,0) as MD_HEPATOBILIARY,
	MD_CHOLECYSTECTOMY::NUMBER(18,0) as MD_CHOLECYSTECTOMY,
	MD_TOTAL_HYSTERECTOMY::NUMBER(18,0) as MD_TOTAL_HYSTERECTOMY,
	MD_MYOMECTOMY::NUMBER(18,0) as MD_MYOMECTOMY,
	MD_C_SECTION::NUMBER(18,0) as MD_C_SECTION,
	MD_NORMAL_DELIVERY::NUMBER(18,0) as MD_NORMAL_DELIVERY,
	MD_CABG::NUMBER(18,0) as MD_CABG,
	MD_VALVE_REPAIR::NUMBER(18,0) as MD_VALVE_REPAIR,
	MD_ABDOMINAL::NUMBER(18,0) as MD_ABDOMINAL,
	MD_BREAST_RECONSTRUCTION::NUMBER(18,0) as MD_BREAST_RECONSTRUCTION,
	MD_ORAL_CRANIAL_MAXILOFACIAL::NUMBER(18,0) as MD_ORAL_CRANIAL_MAXILOFACIAL,
	HCP_TYPE::VARCHAR(255) as HCP_TYPE,
	REMARKS::VARCHAR(255) as REMARKS,
	ACCOUNT_CLASSIFICATION::VARCHAR(255) as ACCOUNT_CLASSIFICATION,
	CUSTOMER_CODE_2::VARCHAR(20) as CUSTOMER_CODE_2,
	INSERTED_DATE::TIMESTAMP_NTZ(9) as INSERTED_DATE,
	UPDATED_DATE::TIMESTAMP_NTZ(9) as UPDATED_DATE,
	PRIMARY_TA::VARCHAR(255) as PRIMARY_TA,
	SECONDARY_TA::VARCHAR(255) as SECONDARY_TA,
	JJ_EXTERNAL_ID::VARCHAR(90) as JJ_EXTERNAL_ID,
	SPECIALTY_2::VARCHAR(255) as SPECIALTY_2,
	SUB_SPECIALTY::VARCHAR(255) as SUB_SPECIALTY,
	SEA_ACCOUNT_CLASSIFICATION::VARCHAR(255) as SEA_ACCOUNT_CLASSIFICATION,
	JJ_EMAIL_1::VARCHAR(80) as JJ_EMAIL_1,
	JJ_EMAIL_2::VARCHAR(80) as JJ_EMAIL_2,
    from t1
)
SELECT *
FROM final