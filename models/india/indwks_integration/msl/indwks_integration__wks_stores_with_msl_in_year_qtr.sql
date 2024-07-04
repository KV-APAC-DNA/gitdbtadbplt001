with WKS_RPT_SSS_SCORECARD_BASE as(
    select * from {{ ref('indwks_integration__wks_rpt_sss_scorecard_base') }}
),
WKS_RPT_SSS_SCORECARD as(
    select * from {{ ref('indwks_integration__wks_rpt_sss_scorecard') }}
),
ITG_QUERY_PARAMETERS as(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
sto as(
    SELECT STORE_CODE,
    YEAR,
    QUARTER
    FROM (
        SELECT STORE_CODE,
            YEAR,
            QUARTER,
            KPI
        FROM WKS_RPT_SSS_SCORECARD_BASE SSS
        GROUP BY STORE_CODE,
            YEAR,
            QUARTER,
            KPI
        ) TEMP
    WHERE KPI IN --('MSL') --mandatory MSL check removed
        (
            SELECT UPPER(PARAMETER_VALUE)
            FROM ITG_QUERY_PARAMETERS
            WHERE UPPER(COUNTRY_CODE) = 'IN'
                AND UPPER(PARAMETER_NAME) IN ('SSS_SCORECARD_KPI_NAME_1', 'SSS_SCORECARD_KPI_NAME_2', 'SSS_SCORECARD_KPI_NAME_3', 'SSS_SCORECARD_KPI_NAME_4', 'SSS_SCORECARD_KPI_NAME_5', 'SSS_SCORECARD_KPI_NAME_6')
                AND UPPER(PARAMETER_TYPE) = 'SSS_KPI'
            )
),
transformed as(
    SELECT STO.STORE_CODE,
        STO.YEAR,
        STO.QUARTER,
        COUNT(DISTINCT SSS.KPI) AS NO_OF_DISTINCT_KPI
    FROM WKS_RPT_SSS_SCORECARD SSS,
        STO
    WHERE STO.STORE_CODE = SSS.STORE_CODE
        AND STO.YEAR = SSS.YEAR
        AND STO.QUARTER = SSS.QUARTER
        AND SSS.KPI <> ('SALES VALUE')
    GROUP BY STO.STORE_CODE,
        STO.YEAR,
        STO.QUARTER
    HAVING COUNT(DISTINCT SSS.KPI) > 1
)
select * from transformed