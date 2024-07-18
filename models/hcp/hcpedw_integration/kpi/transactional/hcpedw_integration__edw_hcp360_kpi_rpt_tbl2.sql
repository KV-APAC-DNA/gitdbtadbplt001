{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where source_system = 'VEEVA';
        {% endif %}"
    )
}}
with 
edw_hcp360_veeva_dim_employee as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_employee') }}
),
edw_hcp360_veeva_fact_call_detail as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_fact_call_detail') }}
),
edw_hcp360_veeva_dim_product_indication as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_product_indication') }}
),
edw_hcp360_veeva_fact_call_key_message as 
(
    select * from {{ source('hcpedw_integration', 'edw_hcp360_veeva_fact_call_key_message') }}
),
edw_hcp360_veeva_dim_key_message as 
(
    select * from {{ source('hcpedw_integration', 'edw_hcp360_veeva_dim_key_message') }}
),
edw_hcp360_veeva_dim_organization as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_organization') }}
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
edw_hcp360_veeva_dim_organization_hcp as 
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_organization_hcp') }}
),
itg_hcp360_veeva_territory as 
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_territory') }}
),
itg_hcp360_veeva_object_territory_association as 
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_object_territory_association') }}
),
edw_vw_hcp360_date_dim as 
(
    select * from {{ ref('hcpedw_integration__edw_vw_hcp360_date_dim') }}
),
tempa as 
(
    SELECT 'IN' AS COUNTRY,
    'VEEVA' AS SOURCE_SYSTEM,
    'DIGITAL' AS CHANNEL,
    'e-Detailing' AS ACTIVITY_TYPE,	
    MASTER.HCP_KEY AS HCP_ID,
    MASTER.HCP_MASTER_ID,
    EMP.EMPLOYEE_SOURCE_ID AS EMPLOYEE_ID,
    MASTER.VEEVA_BRAND AS BRAND
,
    NULL AS BRAND_CATEGORY,
    MASTER.SPECIALITY,
    MASTER.CORE_NONCORE,
    MASTER.CLASSIFICATION,
    MASTER.VHCP_TERRITORY AS TERRITORY,
    MASTER.organization_l3_name AS REGION_HQ,
    MASTER.VHCP_REGION AS REGION,
    MASTER.VHCP_ZONE AS ZONE,
    CAST(NULL AS DATE) HCP_CREATED_DATE,
    to_date(CALL.CALL_DATE_TIME) AS ACTIVITY_DATE,
    CALL.CALL_SOURCE_ID,
    PROD.PRODUCT_NAME AS PRODUCT_INDICATION_NAME,
    0 NO_OF_PRESCRIPTION_UNITS,
    CAST(NULL AS DATE) FIRST_PRESCRIPTION_DATE,
    CAST(CALL.CALL_CLM_FLAG AS NUMERIC) AS PLANNED_CALL_CNT,
    CALL.CALL_DURATION,
    NULL AS DIAGNOSIS,
    NULL AS PRESCRIPTION_ID,
    NULL AS NoofPrescritions,
    NULL AS NoofPrescribers,
    NULL AS EMAIL_NAME,
    NULL AS IS_UNIQUE,
    NULL AS EMAIL_DELIVERED_FLAG,
    NULL AS EMAIL_ACTIVITY_TYPE,
    'Y' AS TRANSACTION_FLAG,
    CALL_KEY.KEY_MESSAGE_NAME AS KEY_MESSAGE,
    current_timestamp() AS CRT_DTTM,
    current_timestamp() AS UPDT_DTTM
FROM 
    EDW_HCP360_VEEVA_DIM_EMPLOYEE EMP,
    EDW_HCP360_VEEVA_FACT_CALL_DETAIL CALL,
    EDW_HCP360_VEEVA_DIM_PRODUCT_INDICATION PROD,
    (
        SELECT DISTINCT COUNTRY_KEY,
            HCP_KEY,
            PRODUCT_INDICATION_KEY,
            CALL_DATE_KEY,
            CALL_DATE,
            CALL_SOURCE_ID,
            CLM_ID,
            CALL_KEY_MESSAGE_ID,
            KM.KEY_MESSAGE_NAME
        FROM 
            EDW_HCP360_VEEVA_FACT_CALL_KEY_MESSAGE CK,
            EDW_HCP360_VEEVA_DIM_KEY_MESSAGE KM
        WHERE CK.CALL_KEY_MESSAGE_ID = KM.KEY_MESSAGE_SOURCE_ID
    ) CALL_KEY,
    (
        SELECT ORGANIZATION_L2_NAME,
            ORGANIZATION_L3_NAME,
            ORGANIZATION_L4_NAME,
            ORGANIZATION_L5_NAME,
            ORG.ORGANIZATION_KEY,
            ORG.COUNTRY_CODE
        FROM 
            EDW_HCP360_VEEVA_DIM_ORGANIZATION ORG,
            ITG_HCP360_VEEVA_TERRITORY T
        WHERE ORG.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
            AND organization_l5_name like 'FBM%'
    ) HEIR,
    (
        SELECT HCP.HCP_SOURCE_ID,
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
        FROM 
            EDW_HCP360_VEEVA_DIM_HCP HCP,
            vw_edw_hcp360_hcpmaster_dim VHCP,
            (
                SELECT OBJECT_ID,
                    CASE
                        WHEN ORGANIZATION_L2_NAME = 'ORSL' THEN 'ORSL'
                        WHEN ORGANIZATION_L2_NAME = 'Johnson Baby Professional' THEN 'JBABY'
                        WHEN ORGANIZATION_L2_NAME = 'DERMA' THEN 'DERMA'
                    END AS VEEVA_BRAND
                FROM 
                    EDW_HCP360_VEEVA_DIM_ORGANIZATION_HCP HCP_ORG,
                    ITG_HCP360_VEEVA_OBJECT_TERRITORY_ASSOCIATION ORG_MAP
                WHERE ORG_MAP.TERRITORY2ID = HCP_ORG.TERRITORY_SOURCE_ID
                    AND ORGANIZATION_L2_NAME IN ('ORSL', 'Johnson Baby Professional', 'DERMA')
            ) AR
        WHERE HCP_MASTER_KEY = VHCP.HCP_MASTER_ID(+)
            AND hcp_source_id = VHCP.hcp_id_veeva(+)
            AND HCP.HCP_SOURCE_ID = AR.OBJECT_ID(+)
            AND HCP_TYPE = 'Doctor'
    ) MASTER
WHERE CALL.HCP_KEY = MASTER.HCP_KEY(+)
    AND CALL.COUNTRY_KEY = MASTER.COUNTRY_CODE (+)
    AND CALL.EMPLOYEE_KEY = EMP.EMPLOYEE_KEY(+)
    AND CALL.COUNTRY_KEY = EMP.COUNTRY_CODE (+)
    AND CALL.PRODUCT_INDICATION_KEY = PROD.PRODUCT_INDICATION_KEY(+)
    AND CALL.COUNTRY_KEY = PROD.COUNTRY_CODE (+)
    AND CALL.ORGANIZATION_KEY = HEIR.ORGANIZATION_KEY(+)
    AND CALL.COUNTRY_KEY = HEIR.COUNTRY_CODE (+)
    AND CALL_STATUS_TYPE = 'Submitted'
    AND CALL_DATE_TIME > '2020-06-30'
    AND CALL.COUNTRY_KEY = CALL_KEY.COUNTRY_KEY(+)
    AND CALL.HCP_KEY = CALL_KEY.HCP_KEY(+)
    AND CALL.PRODUCT_INDICATION_KEY = CALL_KEY.PRODUCT_INDICATION_KEY(+)
    AND CALL.CALL_DATE_KEY = CALL_KEY.CALL_DATE_KEY(+)
    AND CALL.CALL_SOURCE_ID = CALL_KEY.CALL_SOURCE_ID(+)
),
tempb as 
(
    SELECT 
    COUNTRY,
    SOURCE_SYSTEM,
    CHANNEL,
    ACTIVITY_TYPE,
    HCP_ID,
    HCP_MASTER_KEY AS HCP_MASTER_ID,
    EMPLOYEE_ID,
    BRAND,
    BRAND_CATEGORY,
    SPECIALITY,
    CORE_NONCORE,
    CLASSIFICATION,
    TERRITORY,
    REGION_HQ,
    REGION,
    ZONE,
    HCP_CREATED_DATE,
    to_date(CAL_MNTH_ID::varchar, 'YYYYMM') as ACTIVITY_DATE,
    CALL_SOURCE_ID,
    NULL AS PRODUCT_INDICATION_NAME,
    NO_OF_PRESCRIPTION_UNITS,
    FIRST_PRESCRIPTION_DATE,
    PLANNED_CALL_CNT,
    CALL_DURATION,
    DIAGNOSIS,
    PRESCRIPTION_ID,
    NOOFPRESCRITIONS,
    NOOFPRESCRIBERS,
    EMAIL_NAME,
    IS_UNIQUE,
    EMAIL_DELIVERED_FLAG,
    EMAIL_ACTIVITY_TYPE,
    TRANSACTION_FLAG,
    NULL AS KEY_MESSAGE,
    CRT_DTTM,
    UPDT_DTTM
FROM (
        SELECT *
        FROM (
                SELECT 'IN' AS COUNTRY,
                    'VEEVA' AS SOURCE_SYSTEM,
                    'DIGITAL' AS CHANNEL --Check KPI Sheet 				
,
                    'e-Detailing' AS ACTIVITY_TYPE --Check KPI Sheet			
,
                    MASTER.HCP_KEY as HCP_ID,
                    MASTER.HCP_MASTER_KEY,
                    NULL AS EMPLOYEE_ID,
                    MASTER.VEEVA_BRAND AS BRAND --FROM PROD_MAP							
,
                    NULL AS BRAND_CATEGORY,
                    MASTER.SPECIALITY,
                    MASTER.CORE_NONCORE,
                    MASTER.CLASSIFICATION,
                    MASTER.VHCP_TERRITORY AS TERRITORY,
                    MASTER.organization_l3_name AS REGION_HQ,
                    MASTER.VHCP_REGION AS REGION,
                    MASTER.VHCP_ZONE AS ZONE,
                    CAST(NULL AS DATE) HCP_CREATED_DATE,
                    MASTER.HCP_CREATED_DATE AS ACTIVITY_DATE,
                    NULL AS CALL_SOURCE_ID,
                    NULL AS PRODUCT_NAME,
                    0 AS NO_OF_PRESCRIPTION_UNITS,
                    CAST(NULL AS DATE) FIRST_PRESCRIPTION_DATE,
                    0 AS PLANNED_CALL_CNT,
                    NULL AS CALL_DURATION,
                    NULL AS DIAGNOSIS,
                    NULL AS PRESCRIPTION_ID,
                    NULL AS NoofPrescritions,
                    NULL AS NoofPrescribers,
                    NULL AS EMAIL_NAME,
                    NULL AS IS_UNIQUE,
                    NULL AS EMAIL_DELIVERED_FLAG,
                    NULL AS EMAIL_ACTIVITY_TYPE,
                    'N' AS TRANSACTION_FLAG,
                    current_timestamp() AS CRT_DTTM,
                    current_timestamp() AS UPDT_DTTM
                FROM (
                        SELECT HCP.HCP_SOURCE_ID,
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
                        FROM 
                            EDW_HCP360_VEEVA_DIM_HCP HCP,
                            vw_edw_hcp360_hcpmaster_dim VHCP,
                            (
                                SELECT OBJECT_ID,
                                    CASE
                                        WHEN ORGANIZATION_L2_NAME = 'ORSL' THEN 'ORSL'
                                        WHEN ORGANIZATION_L2_NAME = 'Johnson Baby Professional' THEN 'JBABY'
                                        WHEN ORGANIZATION_L2_NAME = 'DERMA' THEN 'DERMA'
                                    END AS VEEVA_BRAND
                                FROM EDW_HCP360_VEEVA_DIM_ORGANIZATION_HCP HCP_ORG,
                                    ITG_HCP360_VEEVA_OBJECT_TERRITORY_ASSOCIATION ORG_MAP
                                WHERE ORG_MAP.TERRITORY2ID = HCP_ORG.TERRITORY_SOURCE_ID
                                    AND ORGANIZATION_L2_NAME IN ('ORSL', 'Johnson Baby Professional', 'DERMA')
                            ) AR
                        WHERE HCP_MASTER_KEY = VHCP.HCP_MASTER_ID(+)
                            AND hcp_source_id = VHCP.hcp_id_veeva(+)
                            AND HCP.HCP_SOURCE_ID = AR.OBJECT_ID(+)
                            AND HCP_TYPE = 'Doctor'
                    ) MASTER
            ) BASE,
            (
                SELECT DISTINCT CAL_MNTH_ID
                FROM EDW_VW_HCP360_DATE_DIM D
                WHERE CAL_DATE >= (convert_timezone('UTC',current_timestamp()))::date - 365
                    AND CAL_MNTH_ID <= TO_CHAR(convert_timezone('UTC',current_timestamp()), 'YYYYMM')::number
            ) CAL
    ) BASE_ALL_MONTHS
WHERE TO_CHAR(ACTIVITY_DATE, 'YYYYMM') <= CAL_MNTH_ID
),
final as 
(
    select * from tempa
    union all
    select * from tempb
)
select * from final