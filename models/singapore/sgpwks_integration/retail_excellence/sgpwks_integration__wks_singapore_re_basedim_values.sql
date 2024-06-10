--overwriding default sql header as we dont want to change timezone to singapore
{{
    config(
        sql_header= ""
    )
}}

--import cte
with wks_singapore_base_retail_excellence as (
    select * from {{ ref('sgpwks_integration__wks_singapore_base_retail_excellence') }}
),
edw_vw_cal_retail_excellence_dim as (
    select * from {{ source('aspedw_integration', 'edw_vw_cal_retail_excellence_dim') }}
),
--final cte
singapore_re_basedim_values  as (
select distinct cntry_cd,
       cntry_nm,
       sellout_dim_key,
       distributor_code,
       distributor_name,
       soldto_code,
       store_code,
       store_name,
       store_type,
       master_code,
       customer_product_desc,
       sap_parent_customer_key,
       sap_parent_customer_description,
       sap_customer_channel_key,
       sap_customer_channel_description,
       sap_customer_sub_channel_key,
       sap_sub_channel_description,
       sap_go_to_mdl_key,
       sap_go_to_mdl_description,
       sap_banner_key,
       sap_banner_description,
       sap_banner_format_key,
       sap_banner_format_description,
       retail_environment,
       region,
       zone_or_area,
       customer_segment_key,
       customer_segment_description,
       global_product_franchise,
       global_product_brand,
       global_product_sub_brand,
       global_product_variant,
       global_product_segment,
       global_product_subsegment,
       global_product_category,
       global_product_subcategory,
       global_put_up_description,
	   mapped_sku_cd,
       --ean,
       --sku_code,sku_description,
       pka_product_key,
       pka_product_key_description
from wks_singapore_base_retail_excellence where mnth_id >= (select last_36mnths		
                        from edw_vw_cal_retail_excellence_dim)::numeric		
      and   mnth_id <= (select prev_mnth from edw_vw_cal_retail_excellence_dim)::numeric		

)

--final select
select * from singapore_re_basedim_values