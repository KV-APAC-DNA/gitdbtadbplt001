with
rx_cx_urc_ventasys_rtl as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_urc_ventasys_rtl') }}
),
rx_cx_sales_achnr_qty as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_sales_achnr_qty') }}
),
final as 
(
    SELECT tmp.*
       ,sales.ach_NR
       ,sales.qty
       ,sales_actv_mnth_cnt
FROM rx_cx_urc_ventasys_rtl tmp
LEFT JOIN (SELECT urc,
                  prod_vent,
                  SUM(ach_NR) AS ach_NR,
                  SUM(qty) AS qty,
                  fisc_yr,
                  qtr,
                  count(NULLIF(round(qty,2),0.00)) AS sales_actv_mnth_cnt
            FROM rx_cx_sales_achnr_qty
            GROUP BY 1,2,5,6) sales
ON tmp.urc::text = sales.urc
AND tmp.rx_product = sales.prod_vent
AND tmp.year = sales.fisc_yr
AND tmp.quarter = sales.qtr
)
select * from final