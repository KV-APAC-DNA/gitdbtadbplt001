with edw_hcp360_veeva_dim_employee as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_veeva_dim_employee
),
edw_hcp360_veeva_fact_call_detail as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_veeva_fact_call_detail
),
edw_hcp360_veeva_dim_product_indication as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_veeva_dim_product_indication
),
edw_hcp360_veeva_dim_hcp as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_veeva_dim_hcp
),
edw_hcp360_veeva_dim_organization as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_veeva_dim_organization
),
itg_hcp360_veeva_territory as
(
    select * from dev_dna_core.snapinditg_integration.itg_hcp360_veeva_territory
),
edw_hcp360_in_ventasys_call_fact as
(
    select * from dev_dna_core.snapindedw_integration.edw_hcp360_in_ventasys_call_fact
),
cte as
(
    SELECT 
        'IN' AS COUNTRY_CODE,
        NULL AS HCP_MASTER_ID,
        HCP_ID,
        EMPLOYEE_ID,
        PRODUCT AS PRODUCT_INDICATION_NAME,
        team_brand_name AS Brand,
        'VENTASYS' AS DATA_SOURCE_NAME,
        CALL_DATE,
        CALL_TYPE,
        NULL AS CALL_DURATION,
        NULL AS PARENT_CALL_FLAG,
        NULL AS PARENT_CALL_SOURCE_ID,
        NULL AS CALL_NAME,
        NULL AS PARENT_CALL_NAME,
        call_id AS CALL_SOURCE_ID,
        CONTACT_TYPE AS VENTASYS_CONTACT_TYPE,
        CRT_DTTM,
        UPDT_DTTM
    FROM EDW_HCP360_IN_VENTASYS_CALL_FACT
),
cte1 as
(
    SELECT
        'IN' AS COUNTRY_CODE,
        HCP.HCP_MASTER_KEY AS HCP_MASTER_ID,
        HCP.HCP_SOURCE_ID AS HCP_ID,
        EMP.EMPLOYEE_SOURCE_ID AS EMPLOYEE_ID,
        PROD.PRODUCT_NAME AS PRODUCT_INDICATION_NAME,
        HEIR.ORGANIZATION_L2_NAME AS Brand,
        'VEEVA' AS DATA_SOURCE_NAME,
        TO_DATE(CALL.CALL_DATE_TIME) AS CALL_DATE,
        CALL.CALL_TYPE,
        CALL.CALL_DURATION,
        CALL.PARENT_CALL_FLAG,
        CALL.PARENT_CALL_SOURCE_ID,
        CALL.CALL_NAME,
        CALL.PARENT_CALL_NAME,
        CALL.CALL_SOURCE_ID,
        NULL AS VENTASYS_CONTACT_TYPE,
        CALL.CRT_DTTM,
        CALL.UPDT_DTTM
    FROM EDW_HCP360_VEEVA_DIM_EMPLOYEE EMP,
        EDW_HCP360_VEEVA_FACT_CALL_DETAIL CALL,
        EDW_HCP360_VEEVA_DIM_PRODUCT_INDICATION PROD,
        EDW_HCP360_VEEVA_DIM_HCP HCP,
        (
            SELECT ORGANIZATION_L3_NAME,
                ORGANIZATION_L4_NAME,
                ORG.ORGANIZATION_KEY,
                ORG.COUNTRY_CODE,
                ORGANIZATION_L2_NAME
            FROM EDW_HCP360_VEEVA_DIM_ORGANIZATION ORG,
                ITG_HCP360_VEEVA_TERRITORY T
            WHERE ORG.TERRITORY_SOURCE_ID = T.TERRITORY_SOURCE_ID
            ) HEIR
    WHERE CALL.HCP_KEY = HCP.HCP_KEY(+)
        AND CALL.COUNTRY_KEY = HCP.COUNTRY_CODE
        AND CALL.EMPLOYEE_KEY = EMP.EMPLOYEE_KEY(+)
        AND CALL.COUNTRY_KEY = EMP.COUNTRY_CODE
        AND CALL.PRODUCT_INDICATION_KEY = PROD.PRODUCT_INDICATION_KEY(+)
        AND CALL.COUNTRY_KEY = PROD.COUNTRY_CODE
        AND CALL.ORGANIZATION_KEY = HEIR.ORGANIZATION_KEY(+)
        AND CALL.COUNTRY_KEY = HEIR.COUNTRY_CODE
),
transformed as 
(
    select * from cte
    union all
    select * from cte1
),
final as 
(   
    select
        country_code::varchar(20) as country_code,
        hcp_master_id::varchar(32) as hcp_master_id,
        hcp_id::varchar(20) as hcp_id,
        employee_id::varchar(20) as employee_id,
        product_indication_name::varchar(50) as product_indication_name,
        brand::varchar(50) as brand,
        data_source_name::varchar(20) as data_source_name,
        call_date::date as call_date,
        call_type::varchar(20) as call_type,
        call_duration::varchar(255) as call_duration,
        parent_call_flag::varchar(20) as parent_call_flag,
        parent_call_source_id::varchar(18) as parent_call_source_id,
        call_name::varchar(80) as call_name,
        parent_call_name::varchar(80) as parent_call_name,
        call_source_id::varchar(20) as call_source_id,
        ventasys_contact_type::varchar(50) as ventasys_contact_type,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from transformed 
)
select * from final 
