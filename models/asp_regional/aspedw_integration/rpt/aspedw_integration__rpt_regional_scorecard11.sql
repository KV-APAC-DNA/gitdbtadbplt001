{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'CIW'
        {% endif %}"
    )
}}
with wks_scorecard_ciw_base as(
    select * from {{ ref('aspwks_integration__wks_scorecard_ciw_base') }}
),
edw_calendar_dim as(
    select * from snapaspedw_integration.edw_calendar_dim
),
final as(
select datasource	,
        ctry_nm	,
        cluster	,
        fisc_yr_per	,
        null as parent_customer	,
		mega_brand,
		0 as copa_nts_usd	,
        0 as copa_nts_lcy	,
        0 as copa_top30_nts_usd	,
        0 as copa_top30_nts_lcy	,
        0 as ecomm_nts_usd	,
        0 as ecomm_nts_lcy	,
        ciw_gts_lcy	,
        ciw_gts_usd	,
        ciw_lcy	,
        ciw_usd	,
        prev_yr_mnth	
        , 0 as nts_prev_yr_mnth
        , 0 as nts_lcy_prev_yr_mnth 
        , 0 as top30_growth
        , 0 as ecomm_nts_prev_yr_mnth
        , 0 as ecomm_nts_lcy_prev_yr_mnth 
        , 0 as ecomm_nts_growth ,       
        ciw_gts_usd_prev_yr_mnth	,
        ciw_usd_prev_yr_mnth,
        ciw_gts_lcy_prev_yr_mnth,
        ciw_lcy_prev_yr_mnth
        ,case when ciw_gts_usd_prev_yr_mnth <> 0 then 
                  round((ciw_gts_usd  - ciw_gts_usd_prev_yr_mnth)/ ciw_gts_usd_prev_yr_mnth,4)
            else 1 
        end  growth_ciw_gts
        ,case when ciw_usd_prev_yr_mnth <> 0 then 
                  round((ciw_usd  - ciw_usd_prev_yr_mnth)/ ciw_usd_prev_yr_mnth,4)
             else 1 
        end  growth_ciw            	
from 
  (
  select 'CIW' as datasource,
           all_months.ctry_nm
         , all_months.cluster
         , all_months.fisc_yr
         , all_months.fisc_yr_per 
         ,all_months.mega_brand
         , ciw_gts_usd
         , ciw_usd
         , ciw_gts_lcy
         , ciw_lcy
         , lag(all_months.fisc_yr_per,12) over (partition by all_months.ctry_nm, all_months.cluster, all_months.ciw_desc,all_months.mega_brand  order by  all_months.fisc_yr_per  ) prev_yr_mnth
         , lag(ciw_gts_usd,12) over (partition by all_months.ctry_nm, all_months.cluster, all_months.ciw_desc,all_months.mega_brand  order by  all_months.fisc_yr_per   ) ciw_gts_usd_prev_yr_mnth
         , lag(ciw_usd,12) over (partition by all_months.ctry_nm, all_months.cluster, all_months.ciw_desc,all_months.mega_brand  order by  all_months.fisc_yr_per   ) ciw_usd_prev_yr_mnth
        , lag(ciw_gts_lcy,12) over (partition by all_months.ctry_nm, all_months.cluster, all_months.ciw_desc,all_months.mega_brand  order by  all_months.fisc_yr_per   ) ciw_gts_lcy_prev_yr_mnth
         , lag(ciw_lcy,12) over (partition by all_months.ctry_nm, all_months.cluster, all_months.ciw_desc,all_months.mega_brand  order by  all_months.fisc_yr_per  ) ciw_lcy_prev_yr_mnth

    from  (
          select distinct ctry_nm, cluster, fisc_yr, ciw_desc,mega_brand
          ,fisc_per  as fisc_yr_per
           from 
             (select   distinct  ctry_nm, cluster ,ciw_desc,mega_brand 
              from wks_scorecard_ciw_base ),
             (select distinct fisc_yr,fisc_per 
               from edw_calendar_dim  
              where fisc_yr >= ( select fisc_yr - 4 from edw_calendar_dim where cal_day =  convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) )
             ) fisc_per 
          ) all_months     
         , wks_scorecard_ciw_base  ciw_base
    where all_months.ctry_nm    = ciw_base.ctry_nm     (+)
     and  all_months.cluster    = ciw_base.cluster     (+)
     and  all_months.fisc_yr_per   = ciw_base.fisc_yr_per (+)
     and  all_months.ciw_desc      = ciw_base.ciw_desc (+) 
     and  all_months.mega_brand      = ciw_base.mega_brand (+)         
  ) ciw
  where fisc_yr >= (select fisc_yr - 2 from edw_calendar_dim where cal_day =  convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) )
)
select * from final