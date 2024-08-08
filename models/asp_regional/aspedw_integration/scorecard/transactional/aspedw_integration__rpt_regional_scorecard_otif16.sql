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
    select * from {{ ref('aspitg_integration__itg_otif_glbl_con_reporting') }}
),
itg_mds_ap_sales_ops_map as(
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
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
   Select base."datasource"  as datasource,
 base."market" as  ctry_nm
, base."cluster" as cluster
, cast(to_char(base.year_month,'YYYY')||'0'||to_char(base.year_month,'MM') as integer) as fisc_yr_per,
null:: VARCHAR(50) as PARENT_CUSTOMER,
null:: VARCHAR(100) as MEGA_BRAND,
null:: NUMBER(38,15) as COPA_NTS_USD,
null:: NUMBER(38,15) as COPA_NTS_LCY,
null:: NUMBER(38,15) as COPA_TOP30_NTS_USD,
null:: NUMBER(38,15) as COPA_TOP30_NTS_LCY,
null:: NUMBER(38,15) as ECOMM_NTS_USD,
null:: NUMBER(38,15) as ECOMM_NTS_LCY,
null:: NUMBER(38,15) as CIW_GTS_LCY,
null:: NUMBER(38,15) as CIW_GTS_USD,
null:: NUMBER(38,15) as CIW_LCY,
null:: NUMBER(38,15) as CIW_USD,
null:: NUMBER(18,0) as PREV_YR_MNTH,
null:: NUMBER(38,15) as NTS_PREV_YR_MNTH,
null:: NUMBER(38,15) as NTS_LCY_PREV_YR_MNTH,
null:: NUMBER(15,5) as TOP30_GROWTH,
null:: NUMBER(38,15) as ECOMM_NTS_PREV_YR_MNTH,
null:: NUMBER(38,15) as ECOMM_NTS_LCY_PREV_YR_MNTH,
null:: NUMBER(15,5) as ECOMM_NTS_GROWTH,
null:: NUMBER(38,15) as CIW_GTS_PREV_YR_MNTH,
null:: NUMBER(38,15) as CIW_PREV_YR_MNTH,
null:: NUMBER(38,15) as CIW_GTS_LCY_PREV_YR_MNTH,
null:: NUMBER(38,15) as CIW_LCY_PREV_YR_MNTH,
null:: NUMBER(38,15) as GROWTH_CIW_GTS,
null:: NUMBER(38,15) as GROWTH_CIW,
base.kpi as KPI
, base."numerator" 
, base."denominator"
, prev_yr."numerator" as prev_yr_numerator
, prev_yr."denominator" as prev_yr_denominator,
null::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR, 
null::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as PROMO_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as SOS_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as SOS_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15)  as SOS_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as SOA_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,5) as HEALTHY_INVENTORY_USD,
null::NUMBER(38,5) as TOTAL_INVENTORY_USD,
null::NUMBER(38,5) as LAST_12_MONTHS_SO_VALUE_USD,
null::NUMBER(8,4) as WEEKS_COVER,
null::DATE as PERFECTSTORE_LATESTDATE,
null::NUMBER(31,2) as DSO_GTS,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE,
null::NUMBER(31,0) as DSO_JNJ_DAYS,
null::VARCHAR(20) as MARKET_SHARE_PERIOD_TYPE,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL,
null::NUMBER(31,2) as MARKET_SHARE_JNJ,
null::NUMBER(31,2) as GROSS_PROFIT,
null::NUMBER(31,2) as FINANCE_NTS,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL_PREV_YR,
null::NUMBER(31,2) as MARKET_SHARE_JNJ_PREV_YR,
null::NUMBER(31,2) as DSO_GTS_PREV_YR,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR,
null:: NUMBER(31,0) as DSO_JNJ_DAYS_PREV_YR,
null:: NUMBER(31,2) as GROSS_PROFIT_PREV_YR,
null:: NUMBER(31,2) as FINANCE_NTS_PREV_YR,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD_PREV_YR_MNTH,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY_PREV_YR_MNTH,
 base."numerator" as NUMERATOR,
base."denominator"  as DENOMINATOR,
prev_yr."numerator" as NUMERATOR_PREV_YR, 
prev_yr."denominator" as DENOMINATOR_PREV_YR
,null:: VARCHAR(20) as CUSTOMER_SEGMENT
,null:: NUMBER(38,15) as CY_SEGMENT_NTS_USD
,null:: NUMBER(38,15) as PY_SEGMENT_NTS_USD
,null:: NUMBER(38,15) as CY_SEGMENT_NTS_LCY
,null:: NUMBER(38,15) as PY_SEGMENT_NTS_LCY          
from otif_base as base
  left join otif_base as prev_yr
 on  base."market" = prev_yr."market" 
 and base."cluster"= prev_yr."cluster"
 and add_months(base.year_month, -12) = prev_yr.year_month
)
select * from final