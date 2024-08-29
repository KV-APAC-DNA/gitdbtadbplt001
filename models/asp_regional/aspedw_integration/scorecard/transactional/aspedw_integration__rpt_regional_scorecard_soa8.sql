with edw_perfect_store_rebase_wt as(
    select * from {{ ref('aspedw_integration__edw_perfect_store_rebase_wt') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
transformed as(
Select 'PERFECTSTORE' as Datasource
      ,Country
      ,cd."cluster"
      ,cast(left(ym,4)||'0'||right(ym,2) as integer) as fiscper
      ,kpi
      ,latestdate 
      , 0 as  MSL_COMPLAINCE_NUMERATOR
      , 0 as  MSL_COMPLAINCE_DENOMINATOR
      , 0 as OSA_COMPLAINCE_NUMERATOR
      , 0 as OSA_COMPLAINCE_DENOMINATOR
      , 0 as  PROMO_COMPLAINCE_NUMERATOR
      , 0 as PROMO_COMPLAINCE_DENOMINATOR
      , 0 as DISPLAY_COMPLAINCE_NUMERATOR
      , 0 as DISPLAY_COMPLAINCE_DENOMINATOR
       ,0 as PLANOGRAM_COMPLAINCE_NUMERATOR
       ,0 as PLANOGRAM_COMPLAINCE_DENOMINATOR
      , 0 as SOS_COMPLAINCE_NUMERATOR         
      , 0 as SOS_COMPLAINCE_DENOMINATOR       
      , SOA_COMPLAINCE_NUMERATOR         
      , SOA_COMPLAINCE_DENOMINATOR 
      , SOA_COMPLAINCE_DENOMINATOR_WT
      From
      (            
          select country, 'SOA COMPLAINCE' as kpi, to_char(scheduleddate,'yyyymm') ym, latestdate,
                          case when sum(num_value) > sum(den_value)  then
                                                  sum(den_value *   weight_soa  ) 
                              else 
                                                  sum(num_value *   weight_soa )
                              end  as SOA_COMPLAINCE_NUMERATOR,
                              sum(den_value) as SOA_COMPLAINCE_DENOMINATOR,
                              sum(den_value * weight_soa)  as SOA_COMPLAINCE_DENOMINATOR_WT
                    
          from
          (
              select case when country='China' then 'China Selfcare' else country end as country, customername, scheduleddate, latestdate, segment,category, sum(num_value) num_value, sum(den_value) den_value, min(weight_soa) as weight_soa
              from
               (   select   country,  customername,  scheduleddate , latestdate, prod_hier_l5 as segment,prod_hier_l4 as category, ques_type,
                            case when ques_type = 'NUMERATOR' and mkt_share is not null 
                                then cast(actual_value as numeric(14,4)) 
                                else null 
                            end as num_value, 
                           case when ques_type = 'DENOMINATOR' 
                                then cast(actual_value as numeric(14,4)) * mkt_share 
                                else null 
                            end as den_value,                
                           round(weight_soa,4) as weight_soa 
                    from  edw_perfect_store_rebase_wt   
                   where  kpi = 'SOA COMPLIANCE' 
                    and priority_store_flag ='Y'                    
                )
                group by  country,  customername,  scheduleddate , latestdate,segment,  category
                having count(distinct ques_type) = 2 
  
           ) group by country, ym ,latestdate
     ) OSA
   ,(select distinct ctry_group,"cluster" from edw_company_dim) cd         
    WHERE OSA.country = cd.ctry_group(+)
),
final as(
select 
Datasource::VARCHAR(50) as DATASOURCE,
country::VARCHAR(40) as CTRY_NM,
"cluster"::VARCHAR(100) as CLUSTER,
fiscper ::NUMBER(18,0) as FISC_YR_PER,  
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
kpi::VARCHAR(50) as KPI,
MSL_COMPLAINCE_NUMERATOR::NUMBER(38,15) as MSL_COMPLAINCE_NUMERATOR,
MSL_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as MSL_COMPLAINCE_DENOMINATOR_WT,
OSA_COMPLAINCE_NUMERATOR::NUMBER(38,15) as OSA_COMPLAINCE_NUMERATOR,
OSA_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as OSA_COMPLAINCE_DENOMINATOR_WT,
PROMO_COMPLAINCE_NUMERATOR::NUMBER(38,15) as PROMO_COMPLAINCE_NUMERATOR,
PROMO_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as PROMO_COMPLAINCE_DENOMINATOR_WT, 
DISPLAY_COMPLAINCE_NUMERATOR::NUMBER(38,15) as DISPLAY_COMPLAINCE_NUMERATOR,
DISPLAY_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as DISPLAY_COMPLAINCE_DENOMINATOR_WT,
PLANOGRAM_COMPLAINCE_NUMERATOR::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_NUMERATOR,
PLANOGRAM_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15) as PLANOGRAM_COMPLAINCE_DENOMINATOR_WT,
SOS_COMPLAINCE_NUMERATOR::NUMBER(38,15) as SOS_COMPLAINCE_NUMERATOR,
SOS_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as SOS_COMPLAINCE_DENOMINATOR,
null::NUMBER(38,15)  as SOS_COMPLAINCE_DENOMINATOR_WT,
SOA_COMPLAINCE_NUMERATOR::NUMBER(38,15) as SOA_COMPLAINCE_NUMERATOR,
SOA_COMPLAINCE_DENOMINATOR::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR,
SOA_COMPLAINCE_DENOMINATOR_WT::NUMBER(38,15) as SOA_COMPLAINCE_DENOMINATOR_WT,
null::NUMBER(38,5) as HEALTHY_INVENTORY_USD,
null::NUMBER(38,5) as TOTAL_INVENTORY_USD,
null::NUMBER(38,5) as LAST_12_MONTHS_SO_VALUE_USD,
null::NUMBER(8,4) as WEEKS_COVER,
latestdate::DATE as PERFECTSTORE_LATESTDATE,
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
from transformed
)
select * from final
