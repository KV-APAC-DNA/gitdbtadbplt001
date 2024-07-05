with v_rpt_sales_details as(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}
),
wks_skurecom_mi_actuals_tmp3 as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_tmp3') }}
),
final as(
    SELECT sf.fisc_yr,
       sf.qtr,
       sf.customer_code,
       sf.retailer_code,
       SUM(sf.achievement_nr) AS achievement_nr_val_br
FROM  v_rpt_sales_details sf
WHERE (sf.fisc_yr,sf.qtr) IN (SELECT year, quarter
                               FROM wks_skurecom_mi_actuals_tmp3
                               GROUP BY 1,2)
GROUP BY sf.fisc_yr,
         sf.qtr,
         sf.customer_code,
         sf.retailer_code
)
select * from final