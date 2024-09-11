{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where rtrim(upper(source_system)) = 'SFMC';
        {% endif %}"
    )
}}
with 
vw_edw_hcp360_hcpmaster_dim as 
(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim') }}
),
edw_hcp360_sfmc_hcp_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_sfmc_hcp_dim') }}
),
edw_hcp360_email_activity_fact as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_email_activity_fact') }}
),
itg_mds_hcp360_hcp_targets_transpose as 
(
    select * from {{ ref('hcpitg_integration__itg_mds_hcp360_hcp_targets_transpose') }}
),

tempa as 
(
    SELECT EAF.CNTRY_CD AS COUNTRY,
             'SFMC' SOURCE_SYSTEM,
             'DIGITAL' CHANNEL,
             'CRM EDM' ACTIVITY_TYPE,
             HCP_DIM.HCP_ID,
             HCP_DIM.HCP_MASTER_ID,
             NULL AS EMPLOYEE_ID,
             case when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) = 'ORSL' then 'ORSL' 
                  when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) = 'JB' then 'JBABY' 
                  when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) = 'Aveeno' then 'DERMA' 
                  when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) not in ('ORSL','JB','Aveeno') then 'ALL BRAND' end AS BRAND,  ---added as new logic 
             NULL AS BRAND_CATEGORY,
             MSTR_DIM.SPECIALITY,
             MSTR_DIM.CORE_NONCORE,
             nullif(MSTR_DIM.CLASSIFICATION,'') as CLASSIFICATION,
             MSTR_DIM.VHCP_TERRITORY AS TERRITORY,
             MSTR_DIM.VHCP_REGION AS REGION,
             MSTR_DIM.organization_l3_name  AS REGION_HQ,
             MSTR_DIM.VHCP_ZONE AS ZONE,
             to_date(HCP_DIM.CREATED_DATE) HCP_CREATED_DATE,
             to_date(EAF.SENT_ACTIVITY_DATE) AS ACTIVITY_DATE,
             NULL AS CALL_SOURCE_ID,
             NULL AS PRODUCT_INDICATION_NAME,
             NULL AS NO_OF_PRESCRIPTION_UNITS,
             NULL AS FIRST_PRESCRIPTION_DATE,
             NULL AS PLANNED_CALL_CNT,
             NULL AS CALL_DURATION,
             NULL AS DIAGNOSIS,
             NULL AS PRESCRIPTION_ID,
             NULL AS NOOFPRESCRITIONS,
             NULL AS NOOFPRESCRIBERS,
             EAF.EMAIL_NAME,
             CASE WHEN EAF.IS_UNIQUE ='True'  then 'Y' 
                  WHEN EAF.IS_UNIQUE ='False' then 'N'
             END as is_unique,
             EAF.EMAIL_DELIVERED_FLAG,
             EAF.ACTIVITY_TYPE as EMAIL_ACTIVITY_TYPE,
			 EMAIL_SUBJECT, 
             current_timestamp() AS CRT_DTTM,
             current_timestamp() AS UPDT_DTTM 
      FROM EDW_HCP360_EMAIL_ACTIVITY_FACT EAF
        LEFT JOIN EDW_HCP360_SFMC_HCP_DIM HCP_DIM
               ON lower(EAF.SUBSCRIBER_KEY) = lower(HCP_DIM.SUBSCRIBER_KEY)
              AND UPPER(EAF.CNTRY_CD) = CASE WHEN UPPER(HCP_DIM.COUNTRY) ='IN'  THEN 'INDIA'
                                             WHEN UPPER(HCP_DIM.COUNTRY) ='INDIA' THEN 'INDIA'
                                         ELSE HCP_DIM.COUNTRY END 
        LEFT JOIN VW_EDW_HCP360_HCPMASTER_DIM MSTR_DIM ON HCP_DIM.HCP_MASTER_ID = MSTR_DIM.HCP_MASTER_ID 
                                                          AND CASE when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) = 'ORSL' then 'ORSL' 
                                                                  when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) = 'JB' then 'JBABY' 
                                                                  when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) = 'Aveeno' then 'DERMA' 
                                                     when SPLIT_PART(EAF.SUBSCRIBER_KEY,'_',1) not in ('ORSL','JB','Aveeno') then 'ALL BRAND' -------added as new logic
                                                              END  = MSTR_DIM.BRAND
             AND NVL(HCP_DIM.SPECIALTY,HCP_DIM.PROFESSION) not in  ('Pharmacist','Pharmacy Assistant')  -- added as part of phase 2 in parameterization
       WHERE (ACTIVITY_DATE) > '2020-09-25'
),
tempb as 
(
    SELECT  COUNTRY,
             'SFMC' as SOURCE_SYSTEM,
             'DIGITAL' as CHANNEL,
             'CRM EDM' as ACTIVITY_TYPE,
             HCP_DIM.HCP_ID,
             HCP_DIM.HCP_MASTER_ID,
             NULL AS EMPLOYEE_ID,
             case when SPLIT_PART(SUBSCRIBER_KEY,'_',1) = 'ORSL' then 'ORSL' 
                  when SPLIT_PART(SUBSCRIBER_KEY,'_',1) = 'JB' then 'JBABY' 
                  when SPLIT_PART(SUBSCRIBER_KEY,'_',1) = 'Aveeno' then 'DERMA' 
                  when SPLIT_PART(SUBSCRIBER_KEY,'_',1) not in ('ORSL','JB','Aveeno') then 'ALL BRAND' end AS BRAND, ---added as new logic
             NULL AS BRAND_CATEGORY,
             MSTR_DIM.SPECIALITY ,
             MSTR_DIM.CORE_NONCORE,
             nullif(MSTR_DIM.CLASSIFICATION,'') as CLASSIFICATION,
             MSTR_DIM.VHCP_TERRITORY as TERRITORY,
             MSTR_DIM.VHCP_REGION as REGION,
             MSTR_DIM.organization_l3_name  as REGION_HQ,
             MSTR_DIM.VHCP_ZONE              as ZONE,
             to_date(HCP_DIM.CREATED_DATE) as HCP_CREATED_DATE,
             NULL AS  ACTIVITY_DATE,
             NULL AS CALL_SOURCE_ID,
             NULL AS PRODUCT_INDICATION_NAME,
             NULL AS NO_OF_PRESCRIPTION_UNITS,
             NULL AS FIRST_PRESCRIPTION_DATE,
             NULL AS PLANNED_CALL_CNT,
             NULL AS CALL_DURATION,
             NULL AS DIAGNOSIS,
             NULL AS PRESCRIPTION_ID,
             NULL AS NOOFPRESCRITIONS,
             NULL AS NOOFPRESCRIBERS,
             NULL AS EMAIL_NAME,
             NULL AS is_unique,
             NULL AS EMAIL_DELIVERED_FLAG,
             NULL AS EMAIL_ACTIVITY_TYPE,
			 NULL AS EMAIL_SUBJECT,
             current_timestamp() AS CRT_DTTM,
             current_timestamp() AS UPDT_DTTM 
      FROM EDW_HCP360_SFMC_HCP_DIM HCP_DIM 
        LEFT JOIN VW_EDW_HCP360_HCPMASTER_DIM MSTR_DIM ON HCP_DIM.HCP_MASTER_ID = MSTR_DIM.HCP_MASTER_ID 
                                                          AND CASE when SPLIT_PART(HCP_DIM.SUBSCRIBER_KEY,'_',1) = 'ORSL' then 'ORSL' 
                                                                  when SPLIT_PART(HCP_DIM.SUBSCRIBER_KEY,'_',1) = 'JB' then 'JBABY' 
                                                                  when SPLIT_PART(HCP_DIM.SUBSCRIBER_KEY,'_',1) = 'Aveeno' then 'DERMA' 
                                                          when SPLIT_PART(SUBSCRIBER_KEY,'_',1) not in ('ORSL','JB','Aveeno') then 'ALL BRAND' ------added as new logic
                                                              END  = MSTR_DIM.BRAND        
             AND NVL(HCP_DIM.SPECIALTY,HCP_DIM.PROFESSION) not in  ('Pharmacist','Pharmacy Assistant')  -- added as part of phase 2 in parameterization
        WHERE  NOT EXISTS ( SELECT 1
                                       FROM EDW_HCP360_EMAIL_ACTIVITY_FACT F
                                      WHERE HCP_DIM.subscriber_key = F.subscriber_key)
),
final as 
(
    select * from tempa
    union all
    select * from tempb
)
select * from final