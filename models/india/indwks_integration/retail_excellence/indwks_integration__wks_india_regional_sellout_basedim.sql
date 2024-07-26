with WKS_INDIA_REGIONAL_SELLOUT_ACT_LM as 
(
select * from {{ ref('indwks_integration__wks_india_regional_sellout_act_lm') }}
),
WKS_INDIA_REGIONAL_SELLOUT_ACT_L3M as 
(
select * from {{ ref('indwks_integration__wks_india_regional_sellout_act_l3m') }}
),
WKS_INDIA_REGIONAL_SELLOUT_ACT_L6M as 
(
select * from {{ ref('indwks_integration__wks_india_regional_sellout_act_l6m') }}
),
WKS_INDIA_REGIONAL_SELLOUT_ACT_L12M as 
(
  select * from  {{ ref('indwks_integration__wks_india_regional_sellout_act_l12m') }}
),

wks_india_regional_sellout_basedim as(

select distinct CNTRY_CD,sellout_dim_key,month from
        (
        select CNTRY_CD,sellout_dim_key,month from WKS_INDIA_REGIONAL_SELLOUT_ACT_LM where LM_SALES is not null
        union all
        select CNTRY_CD,sellout_dim_key,month from  WKS_INDIA_REGIONAL_SELLOUT_ACT_L3M where L3M_SALES is not null
        union all
        select CNTRY_CD,sellout_dim_key,month from  WKS_INDIA_REGIONAL_SELLOUT_ACT_L6M where L6M_SALES is not null
        union all
        select CNTRY_CD,sellout_dim_key,month from  WKS_INDIA_REGIONAL_SELLOUT_ACT_L12M  where L12M_SALES is not null
        )
),
final as(

select 
	cntry_cd ::varchar(2) as cntry_cd,		
	sellout_dim_key :: varchar(32) as sellout_dim_key,		
	month ::varchar(23)	as month	

from wks_india_regional_sellout_basedim

)
select * from final 