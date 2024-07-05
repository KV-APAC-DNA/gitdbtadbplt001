with v_rpt_sales_details as(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}
),
wks_skurecom_mi_actuals_tmp2 as(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_actuals_tmp2') }}
),
final as(
    SELECT sf.fisc_yr,
       sf.qtr,
       sf.customer_code,
       sf.retailer_code,
       sf.mothersku_code,
       SUM(sf.quantity) AS quantity,
       SUM(sf.achievement_nr) AS achievement_nr_val,
       COUNT(DISTINCT sf.num_lines) AS num_lines,
       COUNT(DISTINCT sf.product_code) AS num_packs
FROM  v_rpt_sales_details sf
WHERE (sf.fisc_yr,sf.qtr) IN (SELECT year, quarter
                               FROM wks_skurecom_mi_actuals_tmp2
                               GROUP BY 1,2)
GROUP BY sf.fisc_yr,
         sf.qtr,
         sf.customer_code,
         sf.retailer_code,
         sf.mothersku_code
)
select * from final
