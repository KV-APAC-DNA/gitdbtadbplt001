

WITH hcp_master_aggregation AS (
SELECT *
    FROM
        {{ ref('hcpitg_hcp360_in_detailingdata_dcr_aggregated_hcpmaster_unique_doctors_count') }}
),

 datamart AS (
SELECT *
    FROM
        {{ ref('hcpitg_integration__itg_hcp360_in_detailingdata_dcr_hcp_vaclass_join') }}
),

union1 as (

SELECT

    dd.TEAM,

    dd.MONTH,

    dd.RBM,

    dd.ZBM,

    dd.FBM,

    dd.MSR,

    dd.HQ_CODE,

    dd.MSR_NAME,

    dd.DSG,

    dd.CUSTOMER_NAME,

    dd.CID,

    dd.SPECIALITY,

    dd.CUSTOMER_TYPE,

    dd.ACTIVE,

    dd.DCR_DATE,

    dd.VA_NAME,

    dd.PAGE_NAME,

    dd.PAGE_END_TIME,

    dd.SECONDS,

    dd.CAPPED_SECONDS,

    dd.AS_ON,

    dd.v_custid,

    dd.dcr_dt,

    dd.actual_visit,

    dd.calls_planned,

    dd.calls_done,

    dd.FRANCHISE,

    dd.VA_NAME AS vc_va_name,

    dd.PAGE_NAME AS vc_page_name,

    dd.SUB_GROUP,

    dd.GROUP_NAME,

    dd.BRAND,

    dd.IS_ACTIVE,

    dd.CORE_NONCORE,

    dd.CUST_SPEC,

    NULL AS hcpmaster_core_noncore,

    NULL AS hcpmaster_specialty,

    NULL AS hcpmaster_unique_doctors_count,

    'Datamart' AS data_source

FROM datamart dd
),


union2 as (
SELECT

    NULL AS TEAM,

    NULL AS MONTH,

    NULL AS RBM,

    NULL AS ZBM,

    NULL AS FBM,

    NULL AS MSR,

    NULL AS HQ_CODE,

    NULL AS MSR_NAME,

    NULL AS DSG,

    NULL AS CUSTOMER_NAME,

    NULL AS CID,

    NULL AS SPECIALITY,

    NULL AS CUSTOMER_TYPE,

    NULL AS ACTIVE,

    NULL AS DCR_DATE,

    NULL AS VA_NAME,

    NULL AS PAGE_NAME,

    NULL AS PAGE_END_TIME,

    NULL AS SECONDS,

    NULL AS CAPPED_SECONDS,

    NULL AS AS_ON,
    
    null as    v_custid,
    
    null as    dcr_dt,

    null as    actual_visit,
    
    null as    calls_planned,
    
    null as    calls_done,
    
    null as    FRANCHISE,
    
    null as    VA_NAME ,
    
    null as    PAGE_NAME,
    
    null as    SUB_GROUP,
    
    null as    GROUP_NAME,
    
    null as    BRAND,
    
    null as    IS_ACTIVE,
    
    null as    CORE_NONCORE,
    null as    CUST_SPEC,
    hcp_master_aggregation.hcpmaster_core_noncore,

    hcp_master_aggregation.hcpmaster_specialty,

    hcp_master_aggregation.hcpmaster_unique_doctors_count,

    'HCP Master' AS data_source

FROM hcp_master_aggregation
),

final as 
(select * from union1 union all 
select * from union2 )


select * from final 