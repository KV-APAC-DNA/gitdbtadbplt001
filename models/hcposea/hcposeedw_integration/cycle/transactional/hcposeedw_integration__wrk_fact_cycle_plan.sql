
with itg_cycle_plan as (
select * from {{ ref('hcposeitg_integration__itg_cycle_plan') }}
),
itg_cycle_plan_target as (
select * from {{ ref('hcposeitg_integration__itg_cycle_plan_target') }}
),
dim_hcp as (
select * from {{ ref('hcposeedw_integration__dim_hcp') }}
),
dim_employee as (
select * from {{ ref('hcposeedw_integration__dim_employee') }}
),
dim_date as (
select * from {{ ref('hcposeedw_integration__dim_date') }}
),
dim_country as (
select * from {{ ref('hcposeedw_integration__dim_country') }}
),
dim_organization as (
select * from {{ ref('hcposeedw_integration__dim_organization') }}
),
itg_lookup_retention_period as (
select * from {{ source('hcposeitg_integration', 'itg_lookup_retention_period') }}
),
final as (
SELECT COUNTRY.COUNTRY_KEY,
       P.COUNTRY_CODE,
       EMP.EMPLOYEE_KEY,
       ORG.ORGANIZATION_KEY,
       P.TERRITORY_NAME,
       START_DT.DATE_KEY START_DATE_KEY,
       END_DT.DATE_KEY END_DATE_KEY,
       CAST(IS_ACTIVE AS INTEGER) AS ACTIVE_FLAG,
       P.MANAGER_NAME,
       P.APPROVER_NAME,
       CAST(CLOSE_OUT AS INTEGER)  AS CLOSE_OUT_FLAG,
       CAST(READY_FOR_APPROVAL_FLAG AS INTEGER) AS READY_FOR_APPROVAL_FLAG,
       P.STATUS_TYPE,
       P.CYCLE_PLAN_NAME,
       'SALESREP' CHANNEL_TYPE,
       P.ACTUAL_CALLS ACTUAL_CALL_CNT_CP,
       P.PLANNED_CALLS PLANNED_CALL_CNT_CP,
       P.EXTERNAL_ID AS CYCLE_PLAN_EXTERNAL_ID,
       P.MANAGER,
       P.CP_APPROVAL_TIME,
       P.NUMBER_OF_TARGETS,
       P.NUMBER_OF_CFA_100_TARGETS,
       P.CYCLE_PLAN_ATTAINMENT_CPTARGET,
       P.MID_DATE,
       P.HCP_PRODUCT_ACHIEVED_100,
       P.HCP_PRODUCTS_PLANNED,
       P.CPA_100,
       HCP.HCP_KEY HCP_KEY,
       T.SPECIALTY_1,
       HCP.PARENT_HCO_KEY HCO_KEY,
       T.SCHEDULED_CALLS SCHEDULED_CALL_COUNT,
       T.ACTUAL_CALLS ACTUAL_CALL_COUNT,
       T.PLANNED_CALLS PLANNED_CALL_COUNT,
       T.REMAINING_SCHEDULE REMAINING_CALL_COUNT,
       T.ATTAINMENT,
       T.ORIGINAL_PLANNED_CALLS,
       T.TOTAL_ACTUAL_CALLS,
       T.TOTAL_ATTAINMENT,
       T.TOTAL_PLANNED_CALLS,
       T.EXTERNAL_ID AS CYCLE_PLAN_TARGET_EXTERNAL_ID,
       T.TOTAL_SCHEDULED_CALLS,
       T.REMAINING,
       T.TOTAL_REMAINING,
       T.TOTAL_REMAINING_SCHEDULE,
       T.PRIMARY_PARENT_NAME,
       T.ACCOUNT_SOURCE_ID,
       T.CPT_CFA_100,
       T.CPT_CFA_66,
       T.CPT_CFA_33,
       T.NUMBER_OF_CFA_100_DETAILS,
       T.NUMBER_OF_PRODUCT_DETAILS,
       'TARGET' CYCLE_PLAN_TYPE,
       P.CYCLE_PLAN_SOURCE_ID,
       T.CYCLE_PLAN_TARGET_SOURCE_ID,
       T.CYCLE_PLAN_TARGET_NAME,
       P.LAST_MODIFIED_DATE CYCLE_PLAN_MODIFY_DT,
       P.LAST_MODIFIED_BY_ID CYCLE_PLAN_MODIFY_ID,
       T.LAST_MODIFIED_DATE CYCLE_PLAN_TARGET_MODIFY_DT,
       T.LAST_MODIFIED_BY_ID CYCLE_PLAN_TARGET_MODIFY_ID,
      -- CAST(T.IS_DELETED AS INTEGER)  AS DELETED_FLAG
			 T.CLASSIFICATION
FROM itg_cycle_plan P,
     itg_cycle_plan_target T,
     dim_hcp HCP,
     dim_employee EMP,
     dim_date START_DT,
     dim_date END_DT,
     dim_country COUNTRY,
     dim_organization ORG
WHERE P.CYCLE_PLAN_SOURCE_ID = T.CYCLE_PLAN_VOD_SOURCE_ID
AND   P.COUNTRY_CODE = T.COUNTRY_CODE
AND   P.IS_DELETED =0
AND   T.IS_DELETED =0
AND   NVL(P.START_DATE,current_timestamp()) > (SELECT DATE_TRUNC(YEAR, dateadd(day,-RETENTION_YEARS*365,current_timestamp())) FROM itg_lookup_retention_period WHERE UPPER(TABLE_NAME) = 'FACT_CYCLE_PLAN') 
AND   T.CYCLE_PLAN_ACCOUNT_SOURCE_ID = HCP.HCP_SOURCE_ID(+)
AND   T.COUNTRY_CODE = HCP.COUNTRY_CODE(+)
AND   P.OWNER_SOURCE_ID = EMP.EMPLOYEE_SOURCE_ID(+)
AND   P.COUNTRY_CODE = EMP.COUNTRY_CODE(+)
AND   P.START_DATE = START_DT.DATE_VALUE(+)
AND   P.END_DATE = END_DT.DATE_VALUE(+)
AND   P.COUNTRY_CODE = COUNTRY.COUNTRY_CODE(+)
AND   EMP.MY_ORGANIZATION_CODE = ORG.MY_ORGANIZATION_CODE (+)
AND   TO_DATE(START_DT.DATE_KEY::varchar,'YYYYMMDD') >= NVL(ORG.EFFECTIVE_FROM_DATE, to_date('19000101'::varchar,'YYYYMMDD'))
AND  TO_DATE(START_DT.DATE_KEY::varchar,'YYYYMMDD') <= NVL(ORG.EFFECTIVE_TO_DATE, to_date('20991231'::varchar,'YYYYMMDD'))
)
select * from final