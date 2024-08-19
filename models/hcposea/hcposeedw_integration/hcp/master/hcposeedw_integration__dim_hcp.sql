
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                delete from {{this}}
                where (hcp_key) in (select hcp_key from itg_account_hcp itg_hcp  where itg_hcp.hcp_key = hcp_key);
                    {% endif %}",
                    "{% if is_incremental() %}
                delete from {{this}} where hcp_key ='not applicable';
                    {% endif %}"]
    )
}}

with 
itg_account_hcp as (
select * from PROD_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_ACCOUNT_HCP
),
itg_recordtype as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_RECORDTYPE
),
itg_account_hco as (
select * from PROD_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_ACCOUNT_HCO
),
itg_lookup_eng_data as (
select * from PROD_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_LOOKUP_ENG_DATA
),
HCP_POS_ENG AS(
SELECT COUNTRY_CODE, KEY_VALUE, TARGET_VALUE
                    FROM itg_lookup_eng_data
                    WHERE  UPPER(TABLE_NAME) ='DIM_HCP'
                    AND UPPER(COLUMN_NAME) = 'POSITION'
                    AND UPPER(TARGET_COLUMN_NAME) = 'POSITION_NAME_ENGLISH')
,
SPEC1_ENG AS(SELECT COUNTRY_CODE, KEY_VALUE, TARGET_VALUE
                    FROM itg_lookup_eng_data
                    WHERE  UPPER(TABLE_NAME) ='DIM_HCP'
                    AND UPPER(COLUMN_NAME) = 'SPECIALTY_1'
                    AND UPPER(TARGET_COLUMN_NAME) = 'SPECIALITY_1_TYPE_ENGLISH')   
,                    
 HCP_TYPE_ENG AS(SELECT COUNTRY_CODE, KEY_VALUE, TARGET_VALUE
                    FROM itg_lookup_eng_data
                    WHERE  UPPER(TABLE_NAME) ='DIM_HCP'
                    AND UPPER(COLUMN_NAME) = 'HCP_TYPE'
                    AND UPPER(TARGET_COLUMN_NAME) = 'HCP_TYPE_ENGLISH') ,
cte1 as (
SELECT DISTINCT ITG_HCP.HCP_KEY,
       ITG_HCP.COUNTRY_CODE,
       ITG_HCP.ACCOUNT_SOURCE_ID,
       ITG_HCP.LAST_MODIFIED_DATE,
       ITG_HCP.LAST_MODIFIED_BY_ID,
       NVL(ITG_HCP.EXTERNAL_ID,'Not Applicable'),
       --ITG_HCP.HCP_TYPE,
       ITG_HCP.PROFESSIONAL_TYPE as HCP_TYPE,
      -- NVL(HCP_TYPE_ENG.TARGET_VALUE,ITG_HCP.HCP_TYPE) AS HCP_TYPE_ENGLISH,
       NVL(HCP_TYPE_ENG.TARGET_VALUE,ITG_HCP.PROFESSIONAL_TYPE) AS HCP_TYPE_ENGLISH,
       ITG_HCP.INACTIVE,
       (CASE WHEN UPPER(ITG_HCP.INACTIVE) = 1 THEN sysdate() WHEN UPPER(ITG_HCP.INACTIVE) IS NULL THEN NULL WHEN UPPER(ITG_HCP.INACTIVE) = ' ' THEN NULL ELSE NULL END) AS INACTIVE_DATE,
       'Not Applicable' AS INACTIVE_REASON,
       ITG_HCP.HCP_NAME,
       ITG_HCP.HCP_ENGLISH_NAME,
       ITG_HCP.HCP_NAME,
       ITG_HCP.GENDER,
       ITG_HCP.BIRTH_DAY,
       ITG_HCP.SPECIALTY_1,
       NVL(SPEC1_ENG.TARGET_VALUE,ITG_HCP.SPECIALTY_1) AS SPECIALITY_1_TYPE_ENGLISH,
       ITG_HCP.POSITION,
       NVL(HCP_POS_ENG.TARGET_VALUE,ITG_HCP.POSITION) AS POSITION_NAME_ENGLISH,
       ITG_HCP.TERRITORY_NAME,
       ITG_HCP.IS_KOL,
       'Not Applicable' AS LICENSE_CD,
       ITG_HCP.DIRECT_LINE,
       ITG_HCP.DIRECT_FAX,
       ITG_HCP.PERSON_EMAIL,
       ITG_HCP.MOBILE_NBR,
       ITG_HCP.DO_NOT_CALL_IND,
       ITG_HCP.EMAIL_OPT_OUT_IND,
       ITG_HCO.HCO_KEY,
       ITG_HCO.HCO_NAME,
       ITG_REC.RECORD_TYPE_NAME,
       NVL(ITG_HCP.EXTERNAL_ID,'Not Applicable') AS HCP_EXTERNAL_ID,
       ITG_HCP.REMARKS,
       ITG_HCP.IS_DELETED,
       ITG_HCP.PREFERRED_NAME,
       ITG_HCP.PROFESSIONAL_TYPE,
       ITG_HCP.GO_CLASSIFICATION,
       ITG_HCP.HCC_ACCOUNT_APPROVED,
       ITG_HCP.SALUTATION,
       ITG_HCP.IS_EXCLUDED_FROM_REALIGN,
       ITG_HCP.SFE_APPROVED,
       ITG_HCP.PHYSICIAN_PRESCRIBING_BEHAVIOR,
       ITG_HCP.PHYSICIAN_BEHAVIORAL_STYLE,
       ITG_HCP.IS_EXTERNAL_ID_NUMBER,
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
       sysdate(),
       sysdate(),
	    ITG_HCP.PRIMARY_TA,
   ITG_HCP.SECONDARY_TA
FROM itg_account_hcp ITG_HCP
  JOIN itg_recordtype ITG_REC ON ITG_HCP.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
       AND ITG_HCP.COUNTRY_CODE = ITG_REC.COUNTRY_CODE 
    LEFT OUTER JOIN itg_account_hco ITG_HCO ON ITG_HCO.ACCOUNT_SOURCE_ID = ITG_HCP.PRIMARY_PARENT_SOURCE_ID 
    AND  ITG_HCO.COUNTRY_CODE = ITG_HCP.COUNTRY_CODE
   LEFT OUTER JOIN HCP_POS_ENG
     ON ITG_HCP.POSITION      = HCP_POS_ENG.KEY_VALUE
     AND ITG_HCP.COUNTRY_CODE = HCP_POS_ENG.COUNTRY_CODE
  LEFT OUTER JOIN SPEC1_ENG
     ON ITG_HCP.SPECIALTY_1  = SPEC1_ENG.KEY_VALUE
    AND ITG_HCP.COUNTRY_CODE = SPEC1_ENG.COUNTRY_CODE
  LEFT OUTER JOIN HCP_TYPE_ENG
    ON ITG_HCP.HCP_TYPE    = HCP_TYPE_ENG.KEY_VALUE
   AND ITG_HCP.COUNTRY_CODE = HCP_TYPE_ENG.COUNTRY_CODE),
   
cte2 as (
SELECT 'Not Applicable',
       'ZZ',
       'NA',
      sysdate(),
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       sysdate(),
       'NOT APPLICABLE',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       sysdate(),
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       'Not Applicable',
       0,
       0,
       0,
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       sysdate(),
       sysdate(),
	   'Not Applicable',
	   'Not Applicable'
),
final as (
select * from cte1
union all
select * from cte2)
select * from final