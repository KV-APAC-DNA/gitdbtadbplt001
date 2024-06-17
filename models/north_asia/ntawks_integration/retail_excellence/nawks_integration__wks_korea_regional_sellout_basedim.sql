with wks_korea_regional_sellout_act_lm as (
    select * from {{ ref('nawks_integration__wks_korea_regional_sellout_act_lm') }}
),
wks_korea_regional_sellout_act_l3m as (
    select * from {{ ref('nawks_integration__wks_korea_regional_sellout_act_l3m') }}
),
wks_korea_regional_sellout_act_l6m as (
    select * from {{ ref('nawks_integration__wks_korea_regional_sellout_act_l6m') }}
),

wks_korea_regional_sellout_act_l12m as (
    select * from {{ ref('nawks_integration__wks_korea_regional_sellout_act_l12m') }}
),

--final cte
korea_regional_sellout_basedim  as (
SELECT DISTINCT CNTRY_CD,
       dim_key,
	   data_source,
       month
FROM (SELECT CNTRY_CD,
                   dim_key,
                   data_source,
                   MONTH
            FROM WKS_KOREA_REGIONAL_SELLOUT_ACT_LM		
            WHERE (LM_SALES IS NOT NULL OR LM_SALES_QTY IS NOT NULL)
            UNION ALL
            SELECT CNTRY_CD,
                   dim_key,
                   data_source,
                   MONTH
            FROM WKS_KOREA_REGIONAL_SELLOUT_ACT_L3M		
            WHERE (L3M_SALES IS NOT NULL OR L3M_SALES_QTY IS NOT NULL)
            UNION ALL
            SELECT CNTRY_CD,
                   dim_key,
                   data_source,
                   MONTH
            FROM WKS_KOREA_REGIONAL_SELLOUT_ACT_L6M		
            WHERE (L6M_SALES IS NOT NULL OR L6M_SALES_QTY IS NOT NULL)
            UNION ALL
            SELECT CNTRY_CD,
                   dim_key,
                   data_source,
                   MONTH
            FROM WKS_KOREA_REGIONAL_SELLOUT_ACT_L12M		
            WHERE (L12M_SALES IS NOT NULL OR L12M_SALES_QTY IS NOT NULL)

)
),
final as
(
select 
   cntry_cd::varchar(2) AS cntry_cd,
dim_key::varchar(32) AS dim_key,
data_source::varchar(14) AS data_source,
month::varchar(23) AS month

from korea_regional_sellout_basedim
)
--final select
select * from final

