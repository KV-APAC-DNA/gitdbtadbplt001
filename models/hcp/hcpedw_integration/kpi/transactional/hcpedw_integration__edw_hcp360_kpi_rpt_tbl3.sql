{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where source_system = 'VEEVA' and activity_type = 'CME';
        {% endif %}"
    )
}}
with 
edw_hcp360_veeva_fact_event_attendee as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_fact_event_attendee') }}
),
edw_hcp360_veeva_dim_medical_event as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_medical_event') }}
),
edw_hcp360_veeva_dim_hcp as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_hcp') }}
),
vw_edw_hcp360_hcpmaster_dim as 
(
    select * from {{ ref('hcpedw_integration__vw_edw_hcp360_hcpmaster_dim') }}
),
edw_hcp360_veeva_dim_organization_hcp as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_organization_hcp') }}
),
itg_hcp360_veeva_object_territory_association as 
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_object_territory_association') }}
),
edw_hcp360_veeva_fact_survey_response as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_fact_survey_response') }}
),

tempa as 
(
    SELECT 'IN' AS COUNTRY,
       'VEEVA' AS SOURCE_SYSTEM,
       'DIGITAL' AS CHANNEL,
       	'CME' AS ACTIVITY_TYPE,
       --Check KPI Sheet			
       CME.HCP_ID AS HCP_ID,
       MASTER.HCP_MASTER_KEY,
       NULL AS EMPLOYEE_ID,
       MASTER.VEEVA_BRAND as BRAND,
       --FROM PROD_MAP							
       NULL AS BRAND_CATEGORY,
       CME.SPECIALITY,
       MASTER.CORE_NONCORE,
       MASTER.CLASSIFICATION,
       MASTER.VHCP_TERRITORY AS TERRITORY,
       MASTER.organization_l3_name AS REGION_HQ,
       MASTER.VHCP_REGION AS REGION,
       MASTER.VHCP_ZONE AS ZONE,
       CAST(NULL AS DATE) HCP_CREATED_DATE,
       CME.EVENT_DATE AS ACTIVITY_DATE,
       NULL AS CALL_SOURCE_ID,
       NULL AS PRODUCT_INDICATION_NAME,
       CME.MEDICAL_EVENT_SOURCE_ID as MEDICAL_EVENT_ID,
       CME.EVENT_NAME,
       CME.EVENT_TYPE,
       CME.ATTENDEE as ATTENDEE_NAME,
       CME.EVENT_ROLE,
       CME.STATUS as ATTENDEE_STATUS,
       CME.LOCATION as EVENT_LOCATION,
       NULL as SURVEY_NAME,
       NULL as SURVEY_QUESTION,
       NULL as SURVEY_RESPONSE,
       'Y' AS TRANSACTION_FLAG,
       CME.EVENT_STATUS,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM
FROM (SELECT E.ATTENDEE_HCP_KEY AS HCP_ID, 
             E.ACCOUNT_SOURCE_ID AS ACCOUNT_SOURCE_ID,
             M.MEDICAL_EVENT_SOURCE_ID AS MEDICAL_EVENT_SOURCE_ID,
             M.NAME AS EVENT_NAME,
             E.START_DATE AS EVENT_DATE,
             M.EVENT_TYPE AS EVENT_TYPE,
             E.ATTENDEE ,
             E.TW_ROLE AS EVENT_ROLE,
             E.TW_SPECIALTY AS SPECIALITY,
             E.STATUS AS STATUS,
             M.LOCATION AS LOCATION,
             M.CORE_STATUS AS EVENT_STATUS,
              E.COUNTRY_CODE
             FROM EDW_HCP360_VEEVA_FACT_EVENT_ATTENDEE E
        LEFT JOIN EDW_HCP360_VEEVA_DIM_MEDICAL_EVENT M
               ON E.MEDICAL_EVENT_KEY = M.MEDICAL_EVENT_KEY
              AND E.COUNTRY_CODE = M.COUNTRY_CODE
      WHERE E.ATTENDEE_TYPE = 'Account' 
      AND   E.COUNTRY_CODE = 'IN' 
      ) CME
  LEFT JOIN (SELECT HCP.HCP_SOURCE_ID,
                    VHCP.HCP_MASTER_ID,
                    CORE_NONCORE,
                    HCP.HCP_KEY,
                    VHCP.CLASSIFICATION,
                    VHCP.SPECIALITY,
                    HCP.COUNTRY_CODE,
                    HCP.HCP_CREATED_DATE,
                    HCP.HCP_MASTER_KEY,
                    VHCP_TERRITORY,
                    VHCP_REGION,
                    VHCP_ZONE,
                    VHCP.ORGANIZATION_L5_NAME,
                    VHCP.ORGANIZATION_L4_NAME,
                    VHCP.ORGANIZATION_L3_NAME,
                    VHCP.ORGANIZATION_L2_NAME,
                    VHCP.BRAND,
					AR.VEEVA_BRAND
             FROM EDW_HCP360_VEEVA_DIM_HCP HCP,
                  vw_edw_hcp360_hcpmaster_dim VHCP,
				  (SELECT OBJECT_ID, 
                      CASE WHEN ORGANIZATION_L2_NAME = 'ORSL' THEN 'ORSL'
                           WHEN ORGANIZATION_L2_NAME = 'Johnson Baby Professional' THEN 'JBABY'
                           WHEN ORGANIZATION_L2_NAME = 'DERMA' THEN 'DERMA'
                      END AS VEEVA_BRAND
            				   FROM EDW_HCP360_VEEVA_DIM_ORGANIZATION_HCP HCP_ORG
            						,ITG_HCP360_VEEVA_OBJECT_TERRITORY_ASSOCIATION ORG_MAP
            				   WHERE ORG_MAP.TERRITORY2ID = HCP_ORG. TERRITORY_SOURCE_ID 
            				    AND ORGANIZATION_L2_NAME IN ('ORSL','Johnson Baby Professional','DERMA') )AR
             WHERE HCP_MASTER_KEY = VHCP.HCP_MASTER_ID (+)
             AND   hcp_source_id = VHCP.hcp_id_veeva (+)
			 AND HCP.HCP_SOURCE_ID = AR.OBJECT_ID(+)
             --AND   HCP_TYPE = 'Doctor'
			 ) MASTER ON CME.ACCOUNT_SOURCE_ID = MASTER.HCP_SOURCE_ID
                                                AND CME.HCP_ID = MASTER.HCP_KEY  
                                                AND CME.COUNTRY_CODE = MASTER.COUNTRY_CODE
),
tempb as 
(
    SELECT 'IN' AS COUNTRY,
       'VEEVA' AS SOURCE_SYSTEM,
       'DIGITAL' AS CHANNEL,
       				
       'CME' AS ACTIVITY_TYPE,
       --Check KPI Sheet			
       CME.HCP_ID AS HCP_ID,
       MASTER.HCP_MASTER_KEY,
       NULL AS EMPLOYEE_ID,
       MASTER.VEEVA_BRAND as BRAND,
       --FROM PROD_MAP							
       NULL AS BRAND_CATEGORY,
       MASTER.SPECIALITY,
       MASTER.CORE_NONCORE,
       MASTER.CLASSIFICATION,
       MASTER.VHCP_TERRITORY AS TERRITORY,
       MASTER.organization_l3_name AS REGION_HQ,
       MASTER.VHCP_REGION AS REGION,
       MASTER.VHCP_ZONE AS ZONE,
       CAST(NULL AS DATE) HCP_CREATED_DATE,
       to_date(CME.SURVEYDATE) AS ACTIVITY_DATE,
       NULL AS CALL_SOURCE_ID,
       NULL AS PRODUCT_INDICATION_NAME,
       NULL AS MEDICAL_EVENT_ID,
       NULL AS EVENT_NAME,
       NULL AS EVENT_TYPE,
       NULL AS ATTENDEE_NAME,
       NULL AS EVENT_ROLE,
       NULL AS ATTENDEE_STATUS,
       NULL AS EVENT_LOCATION,
       CME.SURVEY_NAME,
       CME.SURVEY_QUESTION,
       CME.RESPONSE AS SURVEY_RESPONSE,
       'Y' AS TRANSACTION_FLAG,
       NULL AS EVENT_STATUS,
       current_timestamp() AS CRT_DTTM,
       current_timestamp() AS UPDT_DTTM
FROM (SELECT R.HCP_KEY AS HCP_ID, 
             R.SURVEY_START_DATE AS SURVEYDATE,
              R.SURVEY_RESPONSE_NAME AS SURVEY_NAME,
              R.SURVEY_QUESTION_TEXT AS SURVEY_QUESTION,
              R.SURVEY_RESPONSE_TEXT AS RESPONSE,
              R.COUNTRY_KEY
             FROM (SELECT HCP_KEY,EMPLOYEE_KEY,COUNTRY_KEY,START_DATE_KEY,SURVEY_START_DATE,SURVEY_RESPONSE_NAME,SURVEY_QUESTION_TEXT,
                           SURVEY_RESPONSE_TEXT,SURVEY_RESPONSE_ORDER,SURVEY_RESPONSE_STATUS,
                            row_number() over (partition by hcp_key order by survey_response_text desc) as rnk
                    FROM EDW_HCP360_VEEVA_FACT_SURVEY_RESPONSE 
                    WHERE SURVEY_QUESTION_TEXT like '%How would you rate the content of the sessions?%'
                    AND SURVEY_RESPONSE_STATUS = 'Submitted_vod'
                    )R
               
      WHERE  R.COUNTRY_KEY = 'IN' and  R.rnk = 1 
      ) CME
  LEFT JOIN (SELECT HCP.HCP_SOURCE_ID,
                    VHCP.HCP_MASTER_ID,
                    CORE_NONCORE,
                    HCP.HCP_KEY,
                    VHCP.CLASSIFICATION,
                    VHCP.SPECIALITY,
                    HCP.COUNTRY_CODE,
                    HCP.HCP_CREATED_DATE,
                    HCP.HCP_MASTER_KEY,
                    VHCP_TERRITORY,
                    VHCP_REGION,
                    VHCP_ZONE,
                    VHCP.ORGANIZATION_L5_NAME,
                    VHCP.ORGANIZATION_L4_NAME,
                    VHCP.ORGANIZATION_L3_NAME,
                    VHCP.ORGANIZATION_L2_NAME,
                    VHCP.BRAND,
					AR.VEEVA_BRAND
					
             FROM EDW_HCP360_VEEVA_DIM_HCP HCP,
                  vw_edw_hcp360_hcpmaster_dim VHCP,
				   (SELECT OBJECT_ID, 
                      CASE WHEN ORGANIZATION_L2_NAME = 'ORSL' THEN 'ORSL'
                           WHEN ORGANIZATION_L2_NAME = 'Johnson Baby Professional' THEN 'JBABY'
                           WHEN ORGANIZATION_L2_NAME = 'DERMA' THEN 'DERMA'
                      END AS VEEVA_BRAND
            				   FROM EDW_HCP360_VEEVA_DIM_ORGANIZATION_HCP HCP_ORG
            						,ITG_HCP360_VEEVA_OBJECT_TERRITORY_ASSOCIATION ORG_MAP
            				   WHERE ORG_MAP.TERRITORY2ID = HCP_ORG. TERRITORY_SOURCE_ID 
            				    AND ORGANIZATION_L2_NAME IN ('ORSL','Johnson Baby Professional','DERMA') )AR
             WHERE HCP_MASTER_KEY = VHCP.HCP_MASTER_ID (+)
             AND   hcp_source_id = VHCP.hcp_id_veeva (+)
			 AND HCP.HCP_SOURCE_ID = AR.OBJECT_ID(+)
             --AND   HCP_TYPE = 'Doctor'
			 ) MASTER ON CME.HCP_ID = MASTER.HCP_KEY  
                                                AND CME.COUNTRY_KEY = MASTER.COUNTRY_CODE
),
final as 
(
    select * from tempa
    union all
    select * from tempb
)
select * from final