{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'ECOMM'
        {% endif %}"
    )
}}
with edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
wks_scorecard_ecomm_base as(
    select * from {{ ref('aspwks_integration__wks_scorecard_ecomm_base') }}
),
final as(
    select Datasource,
        ctry_nm,
        cluster,
        cast(fisc_yr_per as integer) as fisc_yr_per,
        null:: VARCHAR(50) as PARENT_CUSTOMER,
        null:: VARCHAR(100) as MEGA_BRAND,
        0 as copa_nts_usd,
        0 as copa_nts_lcy,
        0 as copa_top30_nts_usd,
        0 as copa_top30_nts_lcy,
        ecomm_nts_usd,
        ecomm_nts_lcy,
        0 as ciw_gts_lcy,
        0 as ciw_gts_usd,
        0 as ciw_lcy,
        0 as ciw_usd,
        cast(prev_yr_mnth	as integer),
        0 as nts_prev_yr_mnth,
        0 as nts_lcy_prev_yr_mnth,         
        0 as top30_growth,       
        ecomm_nts_prev_yr_mnth,
        ecomm_nts_lcy_prev_yr_mnth,
        case when ecomm_nts_prev_yr_mnth <> 0 then 
                  round((ecomm_nts_usd  - ecomm_nts_prev_yr_mnth )/ ecomm_nts_prev_yr_mnth , 4)
             else 1 
        end  ecomm_nts_growth ,
        0 as ciw_gts_prev_yr_mnth,
        0 as ciw_prev_yr_mnth,
        0 as ciw_gts_lcy_prev_yr_mnth,
        0 as ciw_lcy_prev_yr_mnth,        
        0 as growth_ciw_gts,
        0 as growth_ciw,
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
null:: NUMBER(31,2) as NUMERATOR,
null:: NUMBER(31,2) as DENOMINATOR,
null:: NUMBER(31,2) as NUMERATOR_PREV_YR,
null:: NUMBER(31,2) as DENOMINATOR_PREV_YR,
null:: VARCHAR(20) as CUSTOMER_SEGMENT,
null:: NUMBER(38,15) as CY_SEGMENT_NTS_USD,
null:: NUMBER(38,15) as PY_SEGMENT_NTS_USD,
null:: NUMBER(38,15) as CY_SEGMENT_NTS_LCY,
null:: NUMBER(38,15) as PY_SEGMENT_NTS_LCY
from 
(
  select  all_months.Datasource
          , all_months.ctry_nm
          ,  all_months.cluster
          ,  all_months.fisc_year
          ,  all_months.fisc_yr_per
          ,ecomm_nts_usd
          ,ecomm_nts_lcy
         , lag(fisc_yr_per,12) over (partition by all_months.ctry_nm, all_months.cluster   order by  fisc_yr_per  ) prev_yr_mnth
         , lag(ecomm_nts_usd,12) over (partition by all_months.ctry_nm, all_months.cluster order by  fisc_yr_per ) ecomm_nts_prev_yr_mnth
         , lag(ecomm_nts_lcy,12) over (partition by all_months.ctry_nm, all_months.cluster order by  fisc_yr_per ) ecomm_nts_lcy_prev_yr_mnth
    from (
          select distinct Datasource,ctry_nm, cluster, fisc_yr as fisc_year, fisc_per  as fisc_yr_per 
           from 
             (select   distinct  Datasource, ctry_nm, cluster 
              from wks_scorecard_ecomm_base),
             (select distinct fisc_yr,fisc_per 
               from edw_calendar_dim  
              where fisc_yr >= ( select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::DATE)
             ) fisc_per 
         ) all_months
         ,wks_scorecard_ecomm_base ecomm_base
    where all_months.datasource = ecomm_base.datasource (+)
     and  all_months.ctry_nm    = ecomm_base.ctry_nm     (+)
     and  all_months.cluster    = ecomm_base.cluster     (+)
     and  all_months.fisc_yr_per   = ecomm_base.fisc_per (+)          
) ecomm
 where fisc_year >= (select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::DATE)
)
select *
from final