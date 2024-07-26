{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'OTIF-D'
        {% endif %}"
    )
}}
with itg_otif_glbl_con_reporting as(
    select * from snapaspitg_integration.itg_otif_glbl_con_reporting
),
itg_mds_ap_sales_ops_map as(
    select * from snapaspitg_integration.itg_mds_ap_sales_ops_map
),
otif_base as 
    ( SELECT dataset AS "datasource",
       map.destination_market AS "market",
       map.destination_cluster AS "cluster",
	     to_date(left(fiscal_yr_mo,4)||right(fiscal_yr_mo,2),'YYYYMM') as year_month,
       dataset AS kpi,
       SUM(numerator_unit_otifd_delivery) AS "numerator",
       SUM(denom_unit_otifd) AS "denominator" 
  FROM itg_otif_glbl_con_reporting trans
  JOIN itg_mds_ap_sales_ops_map map
   ON trans.country = map.source_market
   AND trans.cluster_name = map.source_cluster
   AND dataset = 'OTIF-D'
WHERE region = 'APAC'
AND   country <> 'JP'
AND   no_charge_ind = 'Revenue'
AND   denom_unit_otifd <> 0
AND   affiliate_flag = '0'
AND   EXTRACT(YEAR FROM convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - 3 < fiscal_yr
GROUP BY map.dataset,
         map.destination_market,
         map.destination_cluster,
		     fiscal_yr_mo
UNION ALL
SELECT dataset AS "datasource",
       map.destination_market AS "market",
       map.destination_cluster AS "cluster",
	     to_date(left(fiscal_yr_mo,4)||right(fiscal_yr_mo,2),'YYYYMM') as year_month,
       dataset AS kpi,
       SUM(num_unit_otifd_ship_confirm) AS "numerator",
       SUM(denom_unit_otifsc) AS "denominator"
  FROM itg_otif_glbl_con_reporting trans
  JOIN itg_mds_ap_sales_ops_map map
   ON trans.country = map.source_market
   AND trans.cluster_name = map.source_cluster
   AND dataset = 'OTIF-D'
WHERE region = 'APAC'
AND   country = 'JP'
AND   no_charge_ind = 'Revenue'
AND   affiliate_flag = '0'
AND   EXTRACT(YEAR FROM convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - 3 < fiscal_yr
GROUP BY map.dataset,
         map.destination_market,
         map.destination_cluster,
		     fiscal_yr_mo ),
final as(
    Select base."datasource"   
  , base."market"                
  , base."cluster"                 
  , cast(to_char(base.year_month,'YYYY')||'0'||to_char(base.year_month,'MM') as integer) fiscper
  , base.kpi
  , base."numerator"                           
  , base."denominator"               
  , prev_yr."numerator" as prev_yr_numerator                
  , prev_yr."denominator" as prev_yr_denominator           
from otif_base as base
  left join otif_base as prev_yr
 on  base."market" = prev_yr."market" 
 and base."cluster"= prev_yr."cluster"
 and add_months(base.year_month, -12)     = prev_yr.year_month
)
select * from final
