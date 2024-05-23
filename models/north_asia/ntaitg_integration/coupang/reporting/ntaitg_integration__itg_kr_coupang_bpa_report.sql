{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " delete from {{this}}
                    where date in  (select distinct date from {{ source('ntasdl_raw', 'sdl_kr_coupang_bpa_report') }});"
                                               )
}}


with sdl_kr_coupang_bpa_report as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_bpa_report') }}
),
final as (
SELECT 
date::varchar(100) as date,
bidding_type::varchar(100) as bidding_type,
sales_method::varchar(100) as sales_method,
campaign_start_date::varchar(100) as campaign_start_date,
campaign_end_date::varchar(100) as campaign_end_date,
ad_objectives::varchar(100) as ad_objectives,
campaign_name::varchar(100) as campaign_name,
campaign_id::varchar(100) as campaign_id,
ad_group::varchar(100) as ad_group,
ad_group_id::varchar(100) as ad_group_id,
ad_name::varchar(100) as ad_name,
template_type::varchar(100) as template_type,
advertisement_id::varchar(100) as advertisement_id,
impression_area::varchar(100) as impression_area,
material_type::varchar(100) as material_type,
material::varchar(100) as material,
material_id::varchar(100) as material_id,
product::varchar(100) as product,
option_id::varchar(100) as option_id,
ad_execution_product_name::varchar(100) as ad_execution_product_name,
ad_execution_option_id::varchar(100) as ad_execution_option_id,
landing_page_type::varchar(100) as landing_page_type,
landing_page_name::varchar(100) as landing_page_name,
landing_page_id::varchar(100) as landing_page_id,
impressed_keywords::varchar(100) as impressed_keywords,
input_keywords::varchar(100) as input_keywords,
keyword_extension_type::varchar(100) as keyword_extension_type,
category::varchar(100) as category,
impression_count::varchar(100) as impression_count,
click_count::varchar(100) as click_count,
ctr::varchar(100) as ctr,
ad_cost::varchar(100) as ad_cost,
total_orders_1d::varchar(100) as total_orders_1d,
direct_orders_1d::varchar(100) as direct_orders_1d,
indirect_orders_1d::varchar(100) as indirect_orders_1d,
total_sales_1d::varchar(100) as total_sales_1d,
direct_sales_qty_1d::varchar(100) as direct_sales_qty_1d,
indirect_sales_qty_1d::varchar(100) as indirect_sales_qty_1d,
total_conversion_sales_1d::varchar(100) as total_conversion_sales_1d,
direct_conversion_sales_1d::varchar(100) as direct_conversion_sales_1d,
indirect_conversion_sales_1d::varchar(100) as indirect_conversion_sales_1d,
total_orders_14d::varchar(100) as total_orders_14d,
direct_orders_14d::varchar(100) as direct_orders_14d,
indirect_orders_14d::varchar(100) as indirect_orders_14d,
total_sales_14d::varchar(100) as total_sales_14d,
direct_sales_qty_14d::varchar(100) as direct_sales_qty_14d,
indirect_sales_qty_14d::varchar(100) as indirect_sales_qty_14d,
total_conversion_sales_14d::varchar(100) as total_conversion_sales_14d,
direct_conversion_sales_14d::varchar(100) as direct_conversion_sales_14d,
indirect_conversion_sales_14d::varchar(100) as indirect_conversion_sales_14d,
total_ad_return_1d::varchar(100) as total_ad_return_1d,
direct_ad_return_1d::varchar(100) as direct_ad_return_1d,
indirect_ad_return_1d::varchar(100) as indirect_ad_return_1d,
total_ad_return_14d::varchar(100) as total_ad_return_14d,
direct_ad_return_14d::varchar(100) as direct_ad_return_14d,
indirect_ad_return_14d::varchar(100) as indirect_ad_return_14d,
file_name::varchar(255) as file_name,
current_timestamp()::timestamp_ntz(9) as crtd_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
FROM sdl_kr_coupang_bpa_report)
select * from final
