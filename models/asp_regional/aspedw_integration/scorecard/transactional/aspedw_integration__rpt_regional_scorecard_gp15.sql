{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'GP'
        {% endif %}"
    )
}}
with v_rpt_rg_total_investment as(
    select * from aspedw_integration.v_rpt_rg_total_investment
),
itg_mds_ap_sales_ops_map as(
    select * from snapaspitg_integration.itg_mds_ap_sales_ops_map
),
gp_base as
  (select   
          ctry_map.destination_market  , 
          ctry_map.destination_cluster ,
          to_date (posting_fiscal_year||lpad(posting_fiscal_period_number,2,'0') ,'YYYYMM') as period,
          kpi,
          case 
              when kpi = 'GP'  then sum(usdf_mtd_amount)   
          end  as  gp_value,
          case
              when kpi = 'NTS' then sum(usdf_mtd_amount)       
          end as  finance_nts 
    from v_rpt_rg_total_investment  gp
       , itg_mds_ap_sales_ops_map ctry_map
  where kpi in ( 'GP' ,'NTS')
     and version_group_code = 'ACT'
     and ctry_map.dataset = 'Total Investment FDW' 
     and ctry_map.source_cluster = gp.cluster_name
     and ctry_map.source_market  = gp.country_name 
   group by ctry_map.destination_cluster , kpi,
            ctry_map.destination_market  , 
            to_date ( (posting_fiscal_year||lpad(posting_fiscal_period_number,2,'0') ), 'YYYYMM')
 ),
 final as(
    select 'GP' as datasource, 
base.destination_market as  ctry_nm, 
base.destination_cluster as cluster ,
cast(to_char(base.period,'YYYY')||'0'||to_char(base.period,'MM') as integer) as fisc_yr_per,
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
null::NUMBER(31,2) as DSO_GTS,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE,
null::NUMBER(31,0) as DSO_JNJ_DAYS,
null::VARCHAR(20) as MARKET_SHARE_PERIOD_TYPE,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL,
null::NUMBER(31,2) as MARKET_SHARE_JNJ,
base.gp_value as GROSS_PROFIT,
base.finance_nts  as FINANCE_NTS,
null::NUMBER(31,2) as MARKET_SHARE_TOTAL_PREV_YR,
null::NUMBER(31,2) as MARKET_SHARE_JNJ_PREV_YR,
null::NUMBER(31,2) as DSO_GTS_PREV_YR,
null::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR,
null:: NUMBER(31,0) as DSO_JNJ_DAYS_PREV_YR,
prev_yr.gp_value as gross_profit_prev_yr,
prev_yr.finance_nts as finance_nts_prev_yr,
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
    from gp_base as base
  left join gp_base as prev_yr
 on  base.destination_market              = prev_yr.destination_market 
 and base.destination_cluster             = prev_yr.destination_cluster
 and base.kpi             = prev_yr.kpi
 and add_months(base.period, -12)     = prev_yr.period
 )
 select * from final