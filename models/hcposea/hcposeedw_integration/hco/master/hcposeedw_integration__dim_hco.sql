{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                delete from {{this}}
                where hco_key in (select hco_key from hcp_osea_itg.itg_account_hco itg_hco  where itg_hco.hco_key = hco_key);
                    {% endif %}",
                    "{% if is_incremental() %}
                delete from {{this}} where hco_key ='Not Applicable';
                    {% endif %}"]
    )
}}


WITH 
itg_lookup_eng_data as (
select * from dev_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_LOOKUP_ENG_DATA
),
itg_account_hco as (
select * from dev_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_ACCOUNT_HCO
),
itg_recordtype as (
select * from dev_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_RECORDTYPE
),
itg_address as (
select * from dev_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_ADDRESS
),
HCO_TYP_ENG AS
(SELECT COUNTRY_CODE, KEY_VALUE, TARGET_VALUE
FROM itg_lookup_eng_data
WHERE UPPER(TABLE_NAME) ='DIM_HCO'
AND UPPER(COLUMN_NAME) = 'HCO_TYPE'
AND UPPER(TARGET_COLUMN_NAME) = 'HCO_TYPE_ENGLISH'
),
cte1 as (
SELECT DISTINCT HCO_KEY,
       ITG_HCO.COUNTRY_CODE,
       ITG_HCO.ACCOUNT_SOURCE_ID,
       NVL(ITG_HCO.LAST_MODIFIED_BY_ID,'Not Applicable') AS LAST_MODIFIED_BY_ID,
       ITG_HCO.LAST_MODIFIED_DATE,
       'Not Applicable' as HCO_BUSINESS_ID,    
       NVL(ITG_HCO.INACTIVE,0),
       'Not Applicable',
       ACCOUNT_NAME,
       HCO_TYPE,
       NVL(TARGET_VALUE,HCO_TYPE) AS HCO_TYPE_ENGLISH_NAME,
       HCO_SECTOR,
       PHONE_NUMBER,
       FAX_NUMBER,
       WEBSITE,
       TERRITORY_NAME,
       HCO_NAME,
       REMARKS,
       IS_EXCLUDED_FROM_REALIGN,
       BEDS,
       TOTAL_MDS_DOS,
       DEPARTMENTS,
       SFE_APPROVED,
       BUSINESS_DESCRIPTION,
       HCC_ACCOUNT_APPROVED,
       TOTAL_PHYSICIANS_ENROLLED,
       TOTAL_PHARMACISTS,
       IS_EXTERNAL_ID_NUMBER,
       OT,
       KAM_PAEDIATRIC,
       KAM_OBGYN,
       KAM_MINIMALLY_INVASIVE,
       KAM_TOTAL_UROLOGYSURGEONS,
       KAM_TOTAL_SURGEONS,
       KAM_TOTAL_RHEUMPHYSICIANS,
       KAM_TOTAL_PSYCHIATRYPHYSICIANS,
       KAM_TOTAL_ORTHOSURGEONS,
       KAM_TOTAL_OPTHALSURGEONS,
       KAM_TOTAL_NEUROLOGYPHYSICIANS,
       KAM_TOTAL_MEDONCOPHYSICIANS,
       KAM_TOTAL_INFECTIOUSPHYSICIANS,
       KAM_TOTAL_HAEMAPHYSICIANS,
       KAM_TOTAL_GENERALSURGEONS,
       KAM_TOTAL_GASTROPHYSICIANS,
       KAM_TOTAL_ENDOPHYSICIANS,
       KAM_TOTAL_DERMAPHYSICIANS,
       KAM_TOTAL_CARDIOLOGYPHYSICIANS,
       KAM_TOTAL_CARDIOSURGEONS,
       KAM_TOTAL_AESTHETICSURGEONS,
       KAM_GENERAL_DIFFERNENTIATIONS,
       KAM_CLINICAL_DIFFERENTIATIONS,
       ITG_REC.RECORD_TYPE_NAME,
       NVL(ITG_HCO.EXTERNAL_ID,'Not Applicable') AS EXTERNAL_ID,
       PARENT_HCO_KEY,
       PARENT_HCO_NAME,
       ITG_HCO.IS_DELETED,
       ADDRESS_NAME,
       ADDRESS_LINE2_NAME,
       SUBURB_TOWN,
       CITY,
       STATE,
       ZIP,
       BRICK,
       MAP,
       NVL(ADDRESS_SOURCE_ID,'Not Applicable') AS ADDRESS_SOURCE_ID,
       APPT_REQUIRED,
       ITG_ADD.EXTERNAL_ID,
       PHONE,
       FAX,
       ITG_ADD.IS_PRIMARY,
       ITG_ADD.INACTIVE,
       CONTROLLING_ADDRESS,
       VEEVA_AUTOGEN_ID,
			 CUSTOMER_CODE,
       sysdate(),
       sysdate()
FROM itg_account_hco ITG_HCO
  JOIN itg_recordtype ITG_REC ON ITG_HCO.RECORD_TYPE_SOURCE_ID = ITG_REC.RECORD_TYPE_SOURCE_ID
  AND ITG_HCO.COUNTRY_CODE = ITG_REC.COUNTRY_CODE
  LEFT OUTER JOIN ( select * from itg_address 
                      where address_source_id in ( select max(address_source_id) from itg_address where is_primary =1 group by account_source_id ) 
                      or address_source_id in ( select max(address_source_id) from itg_address where is_primary =0 
and account_source_id not in ( select account_source_id from itg_address where is_primary =1)  group by account_source_id)
                   )as ITG_ADD ON ITG_ADD.ACCOUNT_SOURCE_ID = decode(ITG_HCO.PRIMARY_PARENT_SOURCE_ID,NULL,ITG_HCO.ACCOUNT_SOURCE_ID,' ',
                   ITG_HCO.ACCOUNT_SOURCE_ID,ITG_HCO.PRIMARY_PARENT_SOURCE_ID )

  AND ITG_HCO.COUNTRY_CODE = ITG_ADD.COUNTRY_CODE

  LEFT OUTER JOIN HCO_TYP_ENG ON ITG_HCO.HCO_TYPE = HCO_TYP_ENG.KEY_VALUE
  AND ITG_HCO.COUNTRY_CODE = HCO_TYP_ENG.COUNTRY_CODE)
,
cte2 as (
SELECT 'Not Applicable',
       'ZZ',
       'Not Applicable',
       'Not Applicable',
       sysdate(),
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
       'Not Applicable',
       0,
       0,
       0,
       0,
       0,
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
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       0,
       'Not Applicable',
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
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       0,
       0,
       'Not Applicable',
       'Not Applicable',
       'Not Applicable',
       sysdate(),
       sysdate()
),
final as (
select * from cte1 
union all
select * from cte2
)
select * from final