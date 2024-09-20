{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'DSO'
        {% endif %}"
    )
}}
with itg_mds_ap_dso as(
    select * from {{ ref('aspitg_integration__itg_mds_ap_dso') }}
),
itg_mds_ap_sales_ops_map as(
    select * from {{ ref('aspitg_integration__itg_mds_ap_sales_ops_map') }}
),
dso_base as 
    (select       
            market,               
           ctry_map.destination_cluster,                 
           to_date ( year||lpad(month,2,'0'),'YYYYMM' ) as year_month,
           sum(gts) as gts,
          sum(gross_account_receivable) as gross_account_receivable,
          max(jnj_days) as jnj_days
    from itg_mds_ap_dso dso
       , itg_mds_ap_sales_ops_map ctry_map 
    where ctry_map.dataset = 'DSO'
      and ctry_map.source_market  = dso.market 
    group by market                
          , ctry_map.destination_cluster                 
          , to_date ( year||lpad(month,2,'0'),'YYYYMM' )  
    ),
final as(
Select 'DSO' as datasource,
base.market as  ctry_nm,
base.destination_cluster as cluster,
cast(to_char(base.year_month,'YYYY')||'0'||to_char(base.year_month,'MM') as integer) as fisc_yr_per,
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
null::VARCHAR(50) as KPI,
null::NUMBER(38,15) as MSL_COMPLAINCE_NUMERATOR,
null::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,15) as OSA_COMPLAINCE_NUMERATOR,
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
base.gts as DSO_GTS,
base.gross_account_receivable  as DSO_GROSS_ACCOUNT_RECEIVABLE,
base.jnj_days as DSO_JNJ_DAYS,
null::VARCHAR(20) as MARKET_SHARE_PERIOD_TYPE,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL,
null::NUMBER(31,2) as MARKET_SHARE_JNJ,
null::NUMBER(31,2) as GROSS_PROFIT,
null::NUMBER(31,2) as FINANCE_NTS,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL_PREV_YR,
null::NUMBER(31,2) as MARKET_SHARE_JNJ_PREV_YR,
null::NUMBER(31,2) as DSO_GTS_PREV_YR,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR,
prev_yr.jnj_days as DSO_JNJ_DAYS_PREV_YR,
null:: NUMBER(31,2) as GROSS_PROFIT_PREV_YR,
null:: NUMBER(31,2) as FINANCE_NTS_PREV_YR,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD_PREV_YR_MNTH,
null:: NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY_PREV_YR_MNTH,
null:: NUMBER(31,2) as NUMERATOR,
null:: NUMBER(31,2) as DENOMINATOR,
null:: NUMBER(31,2) as NUMERATOR_PREV_YR,
null:: NUMBER(31,2) as DENOMINATOR_PREV_YR,
null:: VARCHAR(20) as CUSTOMER_SEGMENT,
null:: NUMBER(38,15) as CY_SEGMENT_NTS_USD,
null:: NUMBER(38,15) as PY_SEGMENT_NTS_USD,
null:: NUMBER(38,15) as CY_SEGMENT_NTS_LCY,
null:: NUMBER(38,15) as PY_SEGMENT_NTS_LCY                 
from dso_base as base
  left join dso_base as prev_yr
 on  base.market                          = prev_yr.market 
 and base.destination_cluster             = prev_yr.destination_cluster
 and add_months(base.year_month, -12)     = prev_yr.year_month
)
select * from final