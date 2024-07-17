with 
itg_hcp360_in_ventasys_rtlmaster as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_rtlmaster') }}
), 
pob_cx_prod_pob_agg as 
(
    select * from {{ ref('hcpwks_integration__pob_cx_prod_pob_agg') }}
),
pob_cx_vent_rtl_achnr_qty as 
(
    select * from {{ ref('hcpwks_integration__pob_cx_vent_rtl_achnr_qty') }}
),
edw_retailer_dim as 
(
    select * from {{ ref('indedw_integration__edw_retailer_dim') }}
),
final as 
(
SELECT COALESCE(rtl.urc::text,sales.urc) AS urc,
       COALESCE(pob.pob_product,sales.prod_vent) AS ventasys_product,
       COALESCE(pob.year,sales.fisc_yr) AS year,
       COALESCE(pob.quarter,sales.qtr) AS quarter,
       COALESCE(pob.month,sales.month) AS month,
       COALESCE(pob.week,sales.week) AS week,
       COALESCE(pob.dcr_dt,sales.invoice_date::text) AS date,
       COALESCE(pob.pob_units,0) AS  pob_units,
       COALESCE(sales.ach_NR,0) AS sales_ach_NR,
       COALESCE(sales.qty,0) AS sales_qty,
       ret.urc_name,
       ret.region_sales,
       ret.territory_sales,
       ret.zone_sales,
       ret.distributor_code,
       ret.distributor_name    
FROM (SELECT urc,
             v_custid_rtl,
             ROW_NUMBER() OVER (PARTITION BY urc ORDER BY v_custid_rtl DESC) AS rnk
       FROM itg_hcp360_in_ventasys_rtlmaster
       WHERE urc IS NOT NULL
      ) rtl
INNER JOIN (SELECT * 
            FROM pob_cx_prod_pob_agg) pob
       ON rtl.v_custid_rtl = pob.v_custid_rtl
      AND rtl.rnk = 1   
FULL OUTER JOIN  pob_cx_vent_rtl_achnr_qty sales
       ON  rtl.urc::text = sales.urc
      AND  pob.pob_product = sales.prod_vent
      AND  pob.dcr_dt = sales.invoice_date
LEFT JOIN (SELECT rtruniquecode  AS urc,
                  retailer_name  AS urc_name,
                  region_name    AS region_sales,
                  territory_name AS territory_sales,
                  zone_name      AS zone_sales,
                  customer_code  AS distributor_code,
                  customer_name  AS distributor_name,
                  ROW_NUMBER() OVER (PARTITION BY rtruniquecode ORDER BY start_date DESC) AS rnk
           FROM edw_retailer_dim
           WHERE actv_flg = 'Y') ret
       ON COALESCE(rtl.urc::text,sales.urc) = ret.urc
      AND ret.rnk = 1
)
select * from final