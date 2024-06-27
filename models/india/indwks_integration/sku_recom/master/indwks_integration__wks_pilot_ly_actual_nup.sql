with v_rpt_sales_details as
(
    select * from snapindedw_integration.v_rpt_sales_details
    --{{ ref('indedw_integration__v_rpt_sales_details') }}
),
wks_pilot_sku_recom_tbl2 as
(
    select * from {{ ref('indwks_integration__wks_pilot_sku_recom_tbl2') }}
),
final as
(
    SELECT sf.mth_mm,
       sf.rtruniquecode,
       COUNT(DISTINCT sf.mothersku_name) AS nup
    FROM  v_rpt_sales_details sf
    WHERE sf.mth_mm IN (SELECT (LEFT(mth_mm,4) - 1)::INT  || RIGHT(mth_mm,2) AS mth_mm
                        FROM wks_pilot_sku_recom_tbl2
                        GROUP BY 1)
    GROUP BY sf.mth_mm,sf.rtruniquecode
)
select * from final