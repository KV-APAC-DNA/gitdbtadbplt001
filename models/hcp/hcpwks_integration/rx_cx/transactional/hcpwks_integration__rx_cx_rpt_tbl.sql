with 
rx_cx_pre_rpt_tbl as 
(
    select * from {{ ref('hcpwks_integration__rx_cx_pre_rpt_tbl') }}
),
salesman_details as 
(
    select * from {{ ref('hcpwks_integration__salesman_details') }}
),
itg_salesperson_mothersku_tmp as 
(
    select * from snapinditg_integration.itg_salesperson_mothersku_tmp
),
final as 
(
    SELECT pre.*, 
       sm.salesman_code_sales,
       sm.salesman_name_sales
FROM rx_cx_pre_rpt_tbl pre
LEFT JOIN salesman_details sm
       ON pre.urc = sm.urc
       AND sm.rnk = 1
WHERE (pre.gtm_flag = 'NO' OR pre.gtm_flag IS NULL)
UNION 
SELECT pre.*, 
       salesman.salesman_code_sales,
       salesman.salesman_name_sales
FROM rx_cx_pre_rpt_tbl pre
LEFT JOIN (SELECT sm.urc,
                  sm.salesman_code_sales,
                  sm.salesman_name_sales,
                  sm.rnk,
                  sku.skuhiervaluecode
           FROM salesman_details sm  
           INNER JOIN itg_salesperson_mothersku_tmp sku
           ON sm.salesman_code_sales = sku.salesmancode
           AND sm.distcode = sku.distrcode) salesman
       ON pre.urc = salesman.urc
       AND pre.franchise_code = salesman.skuhiervaluecode
       AND salesman.rnk = 1
WHERE pre.gtm_flag = 'YES'
)
select * from final