{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " delete from {{this}}
                    where date in  (select distinct date from {{ source('ntasdl_raw', 'sdl_kr_coupang_pa_report') }});"
                                               )
}}


with sdl_kr_coupang_pa_report as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_pa_report') }}
),
final as (
SELECT 
date::varchar(100) as date,
bidding_type::varchar(100) as bidding_type,
sales_method::varchar(100) as sales_method,
ad_types::varchar(100) as ad_types,
campaign_id::varchar(100) as campaign_id,
campaign_name::varchar(100) as campaign_name,
ad_groups::varchar(250) as ad_groups,
ad_execution_product_name::varchar(250) as ad_execution_product_name,
ad_execution_option_id::varchar(100) as ad_execution_option_id,
ad_con_revenue_gen_product_nm::varchar(250) as ad_con_revenue_gen_product_nm,
ad_con_revenue_gen_product_option_id::varchar(100) as ad_con_revenue_gen_product_option_id,
ad_impression_area::varchar(100) as ad_impression_area,
keyword::varchar(100) as keyword,
impression_count::varchar(100) as impression_count,
click_count::varchar(100) as click_count,
ad_cost::varchar(100) as ad_cost,
ctr::varchar(100) as ctr,
total_orders_1d::varchar(100) as total_orders_1d,
direct_orders_1d::varchar(100) as direct_orders_1d,
indirect_orders_1d::varchar(100) as indirect_orders_1d,
total_sales_1d::varchar(100) as total_sales_1d,
direct_sales_quantity_1d::varchar(100) as direct_sales_quantity_1d,
indirect_sales_quantity_1d::varchar(100) as indirect_sales_quantity_1d,
total_conversion_sales_1d::varchar(100) as total_conversion_sales_1d,
direct_conversion_sales_1d::varchar(100) as direct_conversion_sales_1d,
indirect_conversion_sales_1d::varchar(100) as indirect_conversion_sales_1d,
total_orders_14d::varchar(100) as total_orders_14d,
direct_orders_14d::varchar(100) as direct_orders_14d,
indirect_orders_14d::varchar(100) as indirect_orders_14d,
total_sales_14d::varchar(100) as total_sales_14d,
direct_sales_quantity_14d::varchar(100) as direct_sales_quantity_14d,
indirect_sales_quantity_14d::varchar(100) as indirect_sales_quantity_14d,
total_conversion_sales_14d::varchar(100) as total_conversion_sales_14d,
direct_conversion_sales_14d::varchar(100) as direct_conversion_sales_14d,
indirect_conversion_sales_14d::varchar(100) as indirect_conversion_sales_14d,
total_ad_return_1d::varchar(100) as total_ad_return_1d,
direct_ad_return_1d::varchar(100) as direct_ad_return_1d,
indirect_ad_return_1d::varchar(100) as indirect_ad_return_1d,
total_ad_return_14d::varchar(100) as total_ad_return_14d,
direct_ad_return_14d::varchar(100) as direct_ad_return_14d,
indirect_ad_return_14d::varchar(100) as indirect_ad_return_14d,
campaign_start_date::varchar(100) as campaign_start_date,
campaign_end_date::varchar(100) as campaign_end_date,
file_name::varchar(255) as file_name ,
current_timestamp()::timestamp_ntz(9) as crtd_dttm,
current_timestamp()::timestamp_ntz(9) as updt_dttm
FROM sdl_kr_coupang_pa_report)
select * from final
