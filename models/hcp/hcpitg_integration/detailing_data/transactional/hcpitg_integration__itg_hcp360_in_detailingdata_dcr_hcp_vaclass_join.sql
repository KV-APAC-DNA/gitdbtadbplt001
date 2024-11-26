WITH dcr_aggregated AS (

    SELECT *
    FROM
        {{ ref('hcpitg_integration__itg_hcp360_in_detailingdata_dcr_aggregated') }}

),

ventasys_hcp_master AS (
    SELECT *
    FROM {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),

detailing_data AS (
    SELECT * FROM DEV_DNA_LOAD.HCPSDL_RAW.SDL_HCP360_IN_VENTASYS_EDETAILING_RAW
),

va_class AS (
    SELECT * FROM DEV_DNA_LOAD.HCPSDL_RAW.SDL_HCP360_IN_VENTASYS_VA_PAGE_RAW
),


final_transformation AS (

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

        dcr_agg.v_custid,

        dcr_agg.dcr_dt,

        dcr_agg.actual_visit,

        dcr_agg.calls_planned,

        dcr_agg.calls_done,

        vc.FRANCHISE,

        vc.VA_NAME AS vc_va_name,

        vc.PAGE_NAME AS vc_page_name,

        vc.SUB_GROUP,

        vc.GROUP_NAME,

        vc.BRAND,

        hcp.IS_ACTIVE,

        hcp.CORE_NONCORE,

        hcp.CUST_SPEC

    FROM detailing_data dd

    LEFT JOIN dcr_aggregated dcr_agg

       ON 'C' || dd.cid = dcr_agg.v_custid 
       and TO_DATE(dd.dcr_date, 'DD-Mon-YY') = dcr_agg.dcr_dt 

    LEFT JOIN ventasys_hcp_master hcp

        ON  CONCAT('C', dd.cid) = hcp.V_CUSTID

    LEFT JOIN va_class vc

        ON dd.VA_NAME = vc.VA_NAME

        AND dd.PAGE_NAME = vc.PAGE_NAME

   -- WHERE TO_DATE(dd.dcr_date, 'DD-Mon-YY') = dcr_agg.dcr_dt

)

SELECT *
FROM final_transformation 









