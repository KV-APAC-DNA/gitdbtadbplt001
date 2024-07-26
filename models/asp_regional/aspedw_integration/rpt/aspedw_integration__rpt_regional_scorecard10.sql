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
    select * from snapaspedw_integration.edw_calendar_dim
),
wks_scorecard_ecomm_base as(
    select * from {{ ref('aspwks_integration__wks_scorecard_ecomm_base') }}
),
final as(
    select Datasource,
        ctry_nm,
        cluster,
        cast(fisc_yr_per as integer),
        null as parent_customer,
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
        0 as growth_ciw           	
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
              where fisc_yr >= ( select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::timestamp_ntz(9))
             ) fisc_per 
         ) all_months
         ,wks_scorecard_ecomm_base ecomm_base
    where all_months.datasource = ecomm_base.datasource (+)
     and  all_months.ctry_nm    = ecomm_base.ctry_nm     (+)
     and  all_months.cluster    = ecomm_base.cluster     (+)
     and  all_months.fisc_yr_per   = ecomm_base.fisc_per (+)          
) ecomm
 where fisc_year >= (select fisc_yr - 4 from rg_edw.edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::timestamp_ntz(9))
)
select * from final