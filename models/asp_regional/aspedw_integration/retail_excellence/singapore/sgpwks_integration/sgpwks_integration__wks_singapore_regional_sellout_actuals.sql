--overwriding default sql header as we dont want to change timezone to singapore
{{
    config(
        sql_header= ""
    )
}}

--import cte
with edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
wks_singapore_regional_sellout_act_lm as (
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

wks_singapore_base_retail_excellence as (
    select * from {{ ref('sgpwks_integration__wks_singapore_base_retail_excellence') }}
),

wks_singapore_regional_sellout_basedim as (
    select * from {{ ref('sgpwks_integration__wks_singapore_regional_sellout_basedim') }}
),

wks_singapore_re_basedim_values as (
    select * from {{ ref('sgpwks_integration__wks_singapore_re_basedim_values') }}
),



--final cte
singapore_regional_sellout_actuals  as (
select re_base_dim.cntry_cd,		
       re_base_dim.cntry_nm,		
       substring(base_dim.month,1,4) as year,		
       base_dim.month as mnth_id,		
       re_base_dim.distributor_code,		
       re_base_dim.soldto_code,	
       re_base_dim.distributor_name,		
       re_base_dim.store_code,		
       re_base_dim.store_name,		
       re_base_dim.store_type,		
       re_base_dim.master_code,		
       re_base_dim.customer_product_desc as master_code_desc,		
       re_base_dim.sap_parent_customer_key,		
       re_base_dim.sap_parent_customer_description,		
       re_base_dim.sap_customer_channel_key,		
       re_base_dim.sap_customer_channel_description,		
       re_base_dim.sap_customer_sub_channel_key,		
       re_base_dim.sap_sub_channel_description,		
       re_base_dim.sap_go_to_mdl_key,		
       re_base_dim.sap_go_to_mdl_description,		
       re_base_dim.sap_banner_key,		
       re_base_dim.sap_banner_description,		
       re_base_dim.sap_banner_format_key,		
       re_base_dim.sap_banner_format_description,		
       re_base_dim.retail_environment,		
       re_base_dim.region,		
       re_base_dim.zone_or_area,		
       re_base_dim.customer_segment_key,		
       re_base_dim.customer_segment_description,		
       re_base_dim.global_product_franchise,		
       re_base_dim.global_product_brand,		
       re_base_dim.global_product_sub_brand,		
       re_base_dim.global_product_variant,		
       re_base_dim.global_product_segment,		
       re_base_dim.global_product_subsegment,		
       re_base_dim.global_product_category,		
       re_base_dim.global_product_subcategory,		
       re_base_dim.global_put_up_description,		
	   re_base_dim.mapped_sku_cd,		
       --ean,
       --sku_code,sku_description,
       re_base_dim.pka_product_key,		
       re_base_dim.pka_product_key_description,		
       cm.so_sls_qty as cm_sales_qty,		
       cm.so_sls_value as cm_sales,		
       cm.so_avg_qty as cm_avg_sales_qty,		
       cm.sales_value_list_price as cm_sales_value_list_price,		
       lm_sales as lm_sales,
       lm_sales_qty as lm_sales_qty,
       lm_avg_sales_qty as lm_avg_sales_qty,
       lm_sales_lp as lm_sales_lp,
       l3m_sales as p3m_sales,
       l3m_sales_qty as p3m_qty,
       l3m_avg_sales_qty as p3m_avg_qty,
       l3m_sales_lp as p3m_sales_lp,
       f3m_sales as f3m_sales,
       f3m_sales_qty as f3m_qty,
       f3m_avg_sales_qty as f3m_avg_qty,
       l6m_sales as p6m_sales,
       l6m_sales_qty as p6m_qty,
       l6m_avg_sales_qty as p6m_avg_qty,
       l6m_sales_lp as p6m_sales_lp,
       l12m_sales as p12m_sales,
       l12m_sales_qty as p12m_qty,
       l12m_avg_sales_qty as p12m_avg_qty,
       l12m_sales_lp as p12m_sales_lp,
       case
         when lm_sales > 0 then 'y'
         else 'n'
       end as lm_sales_flag,
       case
         when p3m_sales > 0 then 'y'
         else 'n'
       end as p3m_sales_flag,
       case
         when p6m_sales > 0 then 'y'
         else 'n'
       end as p6m_sales_flag,
       case
         when p12m_sales > 0 then 'y'
         else 'n'
       end as p12m_sales_flag,
sysdate()		
from wks_singapore_regional_sellout_basedim base_dim		
  left join wks_singapore_re_basedim_values re_base_dim		
         on re_base_dim.cntry_cd = base_dim.cntry_cd		
        and re_base_dim.sellout_dim_key = base_dim.sellout_dim_key		
  left outer join (select distinct cntry_cd,
                          sellout_dim_key,
                          mnth_id,
                          so_sls_qty,
                          so_sls_value,
                          so_avg_qty,
                          sales_value_list_price
                   from wks_singapore_base_retail_excellence where mnth_id >= (select last_36mnths		
                        from edw_vw_cal_retail_excellence_dim)::numeric		
      and   mnth_id <= (select prev_mnth from edw_vw_cal_retail_excellence_dim)::numeric) cm		
               on base_dim.cntry_cd = cm.cntry_cd		
              and base_dim.month = cm.mnth_id		
              and base_dim.sellout_dim_key = cm.sellout_dim_key		
  left outer join
--last month
wks_singapore_regional_sellout_act_lm lm
               on base_dim.cntry_cd = lm.cntry_cd		         
              and base_dim.month = lm.month		        
              and base_dim.sellout_dim_key = lm.sellout_dim_key		
  left outer join
--l3m
wks_singapore_regional_sellout_act_l3m l3m
               on base_dim.cntry_cd = l3m.cntry_cd		
              and base_dim.month = l3m.month		
              and base_dim.sellout_dim_key = l3m.sellout_dim_key		
  left outer join
--l6m
wks_singapore_regional_sellout_act_l6m l6m
               on base_dim.cntry_cd = l6m.cntry_cd		
              and base_dim.month = l6m.month		
              and base_dim.sellout_dim_key = l6m.sellout_dim_key		
  left outer join
--l12m
wks_singapore_regional_sellout_act_l12m l12m
               on base_dim.cntry_cd = l12m.cntry_cd		
              and base_dim.month = l12m.month		
              and base_dim.sellout_dim_key = l12m.sellout_dim_key		
where base_dim.month >= (select last_18mnths		
                        from edw_vw_cal_retail_excellence_dim)::numeric		
and   base_dim.month <= (select prev_mnth from edw_vw_cal_retail_excellence_dim)::numeric		

)


--final select
select * from singapore_regional_sellout_actuals