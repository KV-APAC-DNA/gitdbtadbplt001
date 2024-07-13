with WKS_RPT_SSS_SCORECARD_BASE as(
    select * from {{ ref('indwks_integration__wks_rpt_sss_scorecard_base') }}
),
WKS_STORES_WITH_MSL_IN_YEAR_QTR as(
    select * from {{ ref('indwks_integration__wks_stores_with_msl_in_year_qtr') }}
),
transformed as(
    SELECT SSS.TABLE_RN,
        SSS.STORE_CODE,
        SSS.STORE_NAME,
        SSS.PROGRAM_TYPE,
        --NVL(SSS.BRAND,'BRAND NOT APPLICABLE') AS BRAND,
        FRANCHISE,
        NVL(SSS.PROD_HIER_L4, 'No PROD_HIER_L4') AS PROD_HIER_L4,
        SSS.KPI,
        SSS.QUARTER,
        SSS.YEAR,
        NVL(SSS.KPI_SCORE, 0) AS KPI_SCORE
    FROM WKS_RPT_SSS_SCORECARD_BASE SSS,
        WKS_STORES_WITH_MSL_IN_YEAR_QTR STO
    WHERE SSS.STORE_CODE = STO.STORE_CODE
        AND SSS.YEAR = STO.YEAR
        AND SSS.QUARTER = STO.QUARTER
        AND SSS.KPI <> 'SALES VALUE'
)
select * from transformed