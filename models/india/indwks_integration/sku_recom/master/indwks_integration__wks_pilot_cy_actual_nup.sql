with v_rpt_sales_details as
(
    select * from snapindedw_integration.v_rpt_sales_details
    --{{ ref('indedw_integration__v_rpt_sales_details') }}
),
itg_query_parameters as
(
    select * from snapinditg_integration.itg_query_parameters
),
final as 
(
    SELECT sf.mth_mm,
       sf.rtruniquecode,
       COUNT(DISTINCT sf.mothersku_name) AS nup
    FROM  v_rpt_sales_details sf
    WHERE sf.mth_mm >= (SELECT PARAMETER_VALUE AS PARAMETER_VALUE
                        FROM itg_query_parameters
                        WHERE UPPER(COUNTRY_CODE) = 'IN'
                        AND   UPPER(PARAMETER_TYPE) = 'START_PERIOD'
                        AND   UPPER(PARAMETER_NAME) = 'SKU_RECOM_2024_GT_SSS_START_PERIOD')
    GROUP BY sf.mth_mm,sf.rtruniquecode
)
select * from final