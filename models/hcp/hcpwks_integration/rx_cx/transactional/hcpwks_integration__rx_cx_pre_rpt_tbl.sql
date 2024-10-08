with 
itg_rx_cx_target_data as 
(
    select * from {{ ref('hcpitg_integration__itg_rx_cx_target_data') }}
),
itg_rx_cx_pre_target_data as 
(
    select * from {{ ref('hcpitg_integration__itg_rx_cx_pre_target_data') }}
),
sales_actual_achnr_qty as 
(
    select * from {{ ref('hcpwks_integration__sales_actual_achnr_qty') }}
),
retailer_dim_tmp as 
(
    select * from {{ ref('hcpwks_integration__retailer_dim_tmp') }}
),
master_hcp_urc_mapp as 
(
    select * from {{ ref('hcpwks_integration__master_hcp_urc_mapp') }}
),
product_dim_tmp as 
(
    select * from {{ ref('hcpwks_integration__product_dim_tmp') }}
),
udc_gtm_flag_tmp as 
(
    select * from {{ ref('hcpwks_integration__udc_gtm_flag_tmp') }}
),
final as 
(
    SELECT target.urc,
       target.lysq_ach_nr,
       target.lysq_qty,
       target.lysq_presc,
       target.quarter,
       target.target_presc,
       target.target_qty,
       target.case as "case",
       target.prescription_action,
       target.sales_action,
       target.product_vent,
       target.year,
       target.hcp,
       target.prescriptions_needed,
       hcp_master.hcp_name,
       hcp_master.emp_name,
       hcp_master.emp_id,    
       hcp_master.region_vent,
       hcp_master.territory_vent,
       hcp_master.zone_vent,
       coalesce(actl.ach_nr,sales.actual_ach_NR,0) AS actual_ach_NR,  
       coalesce(actl.qty,sales.actual_qty,0) AS actual_qty,
       coalesce(actl.rx_units,0) AS actual_presc,
       ret.urc_name,
       ret.region_sales,
       ret.territory_sales,
       ret.zone_sales,
       ret.distributor_code,
       ret.distributor_name,
       gtm.gtm_flag,
       prod.product_name_sales,
       prod.franchise_code,
       prod.franchise_name
FROM itg_rx_cx_target_data target 
  LEFT JOIN itg_rx_cx_pre_target_data actl
         ON rtrim(target.urc) = rtrim(actl.urc)
        AND rtrim(target.product_vent) = rtrim(actl.rx_product)
        AND rtrim(target.year) = rtrim(actl.year)
        AND trim(target.quarter,'Q') = actl.quarter 
  LEFT JOIN sales_actual_achnr_qty sales
         ON rtrim(target.urc) = rtrim(sales.urc)
        AND rtrim(target.product_vent) = rtrim(sales.prod_vent)
        AND rtrim(target.year) = rtrim(sales.cal_yr)
        AND trim(target.quarter,'Q') = sales.qtr 
  LEFT JOIN retailer_dim_tmp ret
         ON rtrim(target.urc) = rtrim(ret.urc)         
  LEFT JOIN master_hcp_urc_mapp hcp_master
         ON rtrim(target.urc) = rtrim(hcp_master.urc)
  LEFT JOIN product_dim_tmp prod
         ON rtrim(target.product_vent) = rtrim(prod.prod_vent)
  LEFT JOIN (SELECT *, ROW_NUMBER() OVER (PARTITION BY URC order by null) as rnk
             FROM udc_gtm_flag_tmp) gtm
              ON rtrim(target.urc) = rtrim(gtm.urc)
             AND gtm.rnk = 1
)
select * from final