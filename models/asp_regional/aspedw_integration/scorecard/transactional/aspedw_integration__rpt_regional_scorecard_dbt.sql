with aspedw_integration__rpt_regional_scorecardmsl1 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecardmsl1') }}
),
aspedw_integration__rpt_regional_scorecardoos2 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecardoos2') }}
),
aspedw_integration__rpt_regional_scorecard_promo3 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_promo3') }}
),
aspedw_integration__rpt_regional_scorecard_display4 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_display4') }}
),
aspedw_integration__rpt_regional_scorecard_planogram5 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_planogram5') }}
),
aspedw_integration__rpt_regional_scorecard_planogram_compliance6 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_planogram_compliance6') }}
),aspedw_integration__rpt_regional_scorecard_sos7 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_sos7') }}
),aspedw_integration__rpt_regional_scorecard_soa8 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_soa8') }}
),aspedw_integration__rpt_regional_scorecard_inventory_cover9 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_inventory_cover9') }}
),aspedw_integration__rpt_regional_scorecard_copa10 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_copa10') }}
),aspedw_integration__rpt_regional_scorecard_ecomm11 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_ecomm11') }}
),aspedw_integration__rpt_regional_scorecard_ciw12 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_ciw12') }}
),aspedw_integration__rpt_regional_scorecard_market_share13 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_market_share13') }}
),aspedw_integration__rpt_regional_scorecard_ds014 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_ds014') }}
),aspedw_integration__rpt_regional_scorecard_gp15 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_gp15') }}
),aspedw_integration__rpt_regional_scorecard_otif16 as(
    select * from {{ ref('aspedw_integration__rpt_regional_scorecard_otif16') }}
),
final as(
    select * from aspedw_integration__rpt_regional_scorecardmsl1 
    union all 
     select * from aspedw_integration__rpt_regional_scorecardoos2
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_promo3
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_display4
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_planogram5
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_planogram_compliance6
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_sos7
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_soa8
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_inventory_cover9
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_copa10
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_ecomm11
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_ciw12
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_market_share13
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_ds014
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_gp15
    union all 
    select * from aspedw_integration__rpt_regional_scorecard_otif16
)
select DATASOURCE::VARCHAR(50) as DATASOURCE,
CTRY_NM::VARCHAR(40) as CTRY_NM,
CLUSTER::VARCHAR(100) as CLUSTER,
FISC_YR_PER::NUMBER(18,0) as FISC_YR_PER,
PARENT_CUSTOMER::VARCHAR(50) as PARENT_CUSTOMER,
MEGA_BRAND::VARCHAR(100) as MEGA_BRAND,
COPA_NTS_USD::NUMBER(38,15) as COPA_NTS_USD,
COPA_NTS_LCY::NUMBER(38,15) as COPA_NTS_LCY,
COPA_TOP30_NTS_USD::NUMBER(38,15) as COPA_TOP30_NTS_USD,
COPA_TOP30_NTS_LCY::NUMBER(38,15) as COPA_TOP30_NTS_LCY,
ECOMM_NTS_USD::NUMBER(38,15) as ECOMM_NTS_USD,
ECOMM_NTS_LCY::NUMBER(38,15) as ECOMM_NTS_LCY,
CIW_GTS_LCY::NUMBER(38,15) as CIW_GTS_LCY,
CIW_GTS_USD::NUMBER(38,15) as CIW_GTS_USD,
CIW_LCY::NUMBER(38,15) as CIW_LCY,
CIW_USD::NUMBER(38,15) as CIW_USD,
PREV_YR_MNTH::NUMBER(18,0) as PREV_YR_MNTH,
NTS_PREV_YR_MNTH::NUMBER(38,15) as NTS_PREV_YR_MNTH,
NTS_LCY_PREV_YR_MNTH::NUMBER(38,15) as NTS_LCY_PREV_YR_MNTH,
TOP30_GROWTH::NUMBER(15,5) as TOP30_GROWTH,
ECOMM_NTS_PREV_YR_MNTH::NUMBER(38,15) as ECOMM_NTS_PREV_YR_MNTH,
ECOMM_NTS_LCY_PREV_YR_MNTH::NUMBER(38,15) as ECOMM_NTS_LCY_PREV_YR_MNTH,
ECOMM_NTS_GROWTH::NUMBER(15,5) as ECOMM_NTS_GROWTH,
CIW_GTS_PREV_YR_MNTH::NUMBER(38,15) as CIW_GTS_PREV_YR_MNTH,
CIW_PREV_YR_MNTH::NUMBER(38,15) as CIW_PREV_YR_MNTH,
CIW_GTS_LCY_PREV_YR_MNTH::NUMBER(38,15) as CIW_GTS_LCY_PREV_YR_MNTH,
CIW_LCY_PREV_YR_MNTH::NUMBER(38,15) as CIW_LCY_PREV_YR_MNTH,
GROWTH_CIW_GTS::NUMBER(38,15) as GROWTH_CIW_GTS,
GROWTH_CIW::NUMBER(38,15) as GROWTH_CIW,
KPI::VARCHAR(50) as KPI,
MSL_COMPLAINCE_NUMERATOR::NUMBER(38,15) as MSL_COMPLAINCE_NUMERATOR,
MSL_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR,
MSL_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR_WT,
OSA_COMPLAINCE_NUMERATOR::NUMBER(38,15) as OSA_COMPLAINCE_NUMERATOR,
OSA_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR,
OSA_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR_WT,
PROMO_COMPLAINCE_NUMERATOR::NUMBER(38,15) as PROMO_COMPLAINCE_NUMERATOR,
PROMO_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR,
PROMO_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR_WT,
DISPLAY_COMPLAINCE_NUMERATOR::NUMBER(38,15) as DISPLAY_COMPLAINCE_NUMERATOR,
DISPLAY_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR,
DISPLAY_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR_WT,
PLANOGRAM_COMPLAINCE_NUMERATOR::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_NUMERATOR,
PLANOGRAM_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR,
PLANOGRAM_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR_WT,
SOS_COMPLAINCE_NUMERATOR::NUMBER(38,15) as SOS_COMPLAINCE_NUMERATOR,
SOS_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as SOS_COMPLAINCE_DENOMINATOR,
SOS_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as SOS_COMPLAINCE_DENOMINATOR_WT,
SOA_COMPLAINCE_NUMERATOR::NUMBER(38,15) as SOA_COMPLAINCE_NUMERATOR,
SOA_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR,
SOA_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR_WT,
HEALTHY_INVENTORY_USD::NUMBER(38,5) as HEALTHY_INVENTORY_USD,
TOTAL_INVENTORY_USD::NUMBER(38,5) as TOTAL_INVENTORY_USD,
LAST_12_MONTHS_SO_VALUE_USD::NUMBER(38,5) as LAST_12_MONTHS_SO_VALUE_USD,
WEEKS_COVER::NUMBER(8,4) as WEEKS_COVER,
PERFECTSTORE_LATESTDATE::DATE as PERFECTSTORE_LATESTDATE,
DSO_GTS::NUMBER(31,2) as DSO_GTS,
DSO_GROSS_ACCOUNT_RECEIVABLE::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE,
DSO_JNJ_DAYS::NUMBER(31,0) as DSO_JNJ_DAYS,
MARKET_SHARE_PERIOD_TYPE::VARCHAR(20) as MARKET_SHARE_PERIOD_TYPE,
MARKET_SHARE_TOTAL::NUMBER(31,2) as MARKET_SHARE_TOTAL,
MARKET_SHARE_JNJ::NUMBER(31,2) as MARKET_SHARE_JNJ,
GROSS_PROFIT::NUMBER(31,2) as GROSS_PROFIT,
FINANCE_NTS::NUMBER(31,2) as FINANCE_NTS,
MARKET_SHARE_TOTAL_PREV_YR::NUMBER(31,2) as MARKET_SHARE_TOTAL_PREV_YR,
MARKET_SHARE_JNJ_PREV_YR::NUMBER(31,2) as MARKET_SHARE_JNJ_PREV_YR,
DSO_GTS_PREV_YR::NUMBER(31,2) as DSO_GTS_PREV_YR,
DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR::NUMBER(31,2) as DSO_GROSS_ACCOUNT_RECEIVABLE_PREV_YR,
DSO_JNJ_DAYS_PREV_YR::NUMBER(31,0) as DSO_JNJ_DAYS_PREV_YR,
GROSS_PROFIT_PREV_YR::NUMBER(31,2) as GROSS_PROFIT_PREV_YR,
FINANCE_NTS_PREV_YR::NUMBER(31,2) as FINANCE_NTS_PREV_YR,
COPA_NTS_GREENLIGHT_SKU_USD::NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD,
COPA_NTS_GREENLIGHT_SKU_LCY::NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY,
COPA_NTS_GREENLIGHT_SKU_USD_PREV_YR_MNTH::NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_USD_PREV_YR_MNTH,
COPA_NTS_GREENLIGHT_SKU_LCY_PREV_YR_MNTH::NUMBER(38,0) as COPA_NTS_GREENLIGHT_SKU_LCY_PREV_YR_MNTH,
NUMERATOR::NUMBER(31,2) as NUMERATOR,
DENOMINATOR::NUMBER(31,2) as DENOMINATOR,
NUMERATOR_PREV_YR::NUMBER(31,2) as NUMERATOR_PREV_YR,
DENOMINATOR_PREV_YR::NUMBER(31,2) as DENOMINATOR_PREV_YR,
CUSTOMER_SEGMENT::VARCHAR(20) as CUSTOMER_SEGMENT,
CY_SEGMENT_NTS_USD::NUMBER(38,15) as CY_SEGMENT_NTS_USD,
PY_SEGMENT_NTS_USD::NUMBER(38,15) as PY_SEGMENT_NTS_USD,
CY_SEGMENT_NTS_LCY::NUMBER(38,15) as CY_SEGMENT_NTS_LCY,
PY_SEGMENT_NTS_LCY::NUMBER(38,15) as PY_SEGMENT_NTS_LCY from final