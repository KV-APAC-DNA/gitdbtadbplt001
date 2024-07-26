{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where datasource = 'COPA'
        {% endif %}"
    )
}}
with wks_reg_scorecard_copa_base as (
    select * from {{ ref('aspwks_integration__wks_reg_scorecard_copa_base') }}
),
edw_calendar_dim as(
    select * from snapaspedw_integration.edw_calendar_dim
),
final as(
    select   datasource              
        ,ctry_nm                 
        , cluster                 
        , fisc_yr_per             
        , parent_customer  
,mega_brand		
        , copa_nts_usd            
        , copa_nts_lcy       
       ,0 as  copa_top30_nts_usd
       ,0 as copa_top30_nts_lcy
       ,0 as ecomm_nts_usd
       ,0 as ecomm_nts_lcy
       ,0 as  ciw_gts_lcy
       ,0 as  ciw_gts_usd
       ,0 as  ciw_lcy
       ,0 as  ciw_usd      
        --, sum(copa_nts_usd) over (partition by ctry_nm, cluster, parent_customer order by fisc_yr_per rows between  2 preceding and current row ) last_3months_nts 
        , lag(fisc_yr_per,12) over (partition by ctry_nm, cluster, parent_customer,mega_brand  order by  fisc_yr_per  ) prev_yr_mnth
        , lag(copa_nts_usd,12) over (partition by ctry_nm, cluster, parent_customer,mega_brand  order by  fisc_yr_per ) nts_prev_yr_mnth
        , lag(copa_nts_lcy,12) over (partition by ctry_nm, cluster, parent_customer,mega_brand  order by  fisc_yr_per ) nts_lcy_prev_yr_mnth
        , 0 as top30_growth
        , 0 as ecomm_nts_prev_yr_mnth
        , 0 as ecomm_nts_lcy_prev_yr_mnth
        , 0 as ecomm_nts_growth
        , 0 as ciw_gts_prev_yr_mnth	
        , 0 as ciw_prev_yr_mnth
        , 0 as ciw_gts_lcy_prev_yr_mnth 
        , 0 as ciw_lcy_prev_yr_mnth          
        , 0 as growth_ciw_gts
        , 0 as growth_ciw            
     
from
( select all_customer.datasource
         , all_customer.ctry_nm   
         , all_customer.cluster   
         , all_customer.fisc_yr_per 
         , all_customer.parent_customer
		 ,all_customer.mega_brand
         , nvl(base.copa_nts_usd,0) as copa_nts_usd
         , nvl(base.copa_nts_lcy,0) as copa_nts_lcy
  from
        ( select Datasource, ctry_nm, cluster, parent_customer,mega_brand, fisc_per as fisc_yr_per
            from 
                   (select   distinct  Datasource, ctry_nm, cluster, parent_customer ,mega_brand
                      from    wks_reg_scorecard_copa_base ),
                   (select distinct fisc_per 
                      from edw_calendar_dim  
                     where fisc_yr >= ( select fisc_yr - 4 from edw_calendar_dim where cal_day = convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) 
                    ) fisc_per 
         ) all_customer      
      left outer join wks_reg_scorecard_copa_base base
          on  all_customer.datasource =      base.datasource            
         and  all_customer.ctry_nm    =      base.ctry_nm           
         and  all_customer.cluster    =      base.cluster             
         and  all_customer.parent_customer = base.parent_customer 
         and  all_customer.mega_brand = base.mega_brand
         and  all_customer.fisc_yr_per =     base.fisc_yr_per    
 order by datasource              
        ,ctry_nm                 
        , cluster 
        , parent_customer
		,mega_brand
        , fisc_yr_per       
)    
)
select * from final