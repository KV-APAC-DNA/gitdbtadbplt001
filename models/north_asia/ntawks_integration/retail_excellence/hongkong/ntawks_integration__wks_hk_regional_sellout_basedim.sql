--import cte

with wks_hk_regional_sellout_act_lm as (
    select * from {{ ref('ntawks_integration__wks_hk_regional_sellout_act_lm')}}
),
wks_hk_regional_sellout_act_l3m as (
    select * from {{ ref('ntawks_integration__wks_hk_regional_sellout_act_l3m')}}
),
wks_hk_regional_sellout_act_l6m as (
    select * from {{ ref('ntawks_integration__wks_hk_regional_sellout_act_l6m')}}
),
wks_hk_regional_sellout_act_l12m as (
    select * from {{ ref('ntawks_integration__wks_hk_regional_sellout_act_l12m')}}
),

wks_hk_regional_sellout_basedim as 
(
	SELECT DISTINCT CNTRY_CD,
		   dim_key,
		   data_source,
		   month
	FROM (SELECT CNTRY_CD,
					   dim_key,
					   data_source,
					   MONTH
				FROM WKS_HK_REGIONAL_SELLOUT_ACT_LM
				WHERE (LM_SALES IS NOT NULL OR LM_SALES_QTY IS NOT NULL)
				UNION ALL
				SELECT CNTRY_CD,
					   dim_key,
					   data_source,
					   MONTH
				FROM WKS_HK_REGIONAL_SELLOUT_ACT_L3M	
				WHERE (L3M_SALES IS NOT NULL OR L3M_SALES_QTY IS NOT NULL)
				UNION ALL
				SELECT CNTRY_CD,
					   dim_key,
					   data_source,
					   MONTH
				FROM WKS_HK_REGIONAL_SELLOUT_ACT_L6M	
				WHERE (L6M_SALES IS NOT NULL OR L6M_SALES_QTY IS NOT NULL)
				UNION ALL
				SELECT CNTRY_CD,
					   dim_key,
					   data_source,
					   MONTH
				FROM WKS_HK_REGIONAL_SELLOUT_ACT_L12M
				WHERE (L12M_SALES IS NOT NULL OR L12M_SALES_QTY IS NOT NULL))
),

final as 
(
    select cntry_cd :: varchar(2) as cntry_cd,
    dim_key :: varchar(32) as dim_key,
    data_source :: varchar(14) as data_source,
    month :: varchar(27) as month
    from wks_hk_regional_sellout_basedim
)

--final select 

select * from final