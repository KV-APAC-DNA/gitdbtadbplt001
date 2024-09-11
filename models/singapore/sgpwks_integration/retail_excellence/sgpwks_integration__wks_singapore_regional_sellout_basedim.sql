with wks_singapore_regional_sellout_act_lm as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_act_lm') }}
),
wks_singapore_regional_sellout_act_l3m as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_act_l3m') }}
),

wks_singapore_regional_sellout_act_l6m as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_act_l6m') }}
),

wks_singapore_regional_sellout_act_l12m as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_act_l12m') }}
),


--final cte
singapore_regional_sellout_basedim  as (
select distinct cntry_cd,
       sellout_dim_key,
       month
from (select cntry_cd,
             sellout_dim_key,
             month
      from wks_singapore_regional_sellout_act_lm		
      where lm_sales is not null
     union all
     select cntry_cd,
            sellout_dim_key,
           month
      from wks_singapore_regional_sellout_act_l3m		
      where l3m_sales is not null
      union all
      select cntry_cd,
             sellout_dim_key,
             month
      from wks_singapore_regional_sellout_act_l6m		
      where l6m_sales is not null
      union all
      select cntry_cd,
             sellout_dim_key,
             month
      from wks_singapore_regional_sellout_act_l12m		
      where l12m_sales is not null)

),
final as
(
    select 
    cntry_cd::varchar(2) AS cntry_cd,
sellout_dim_key::varchar(32) AS sellout_dim_key,
month::varchar(23) AS month
from singapore_regional_sellout_basedim
)

--final select
select * from final

