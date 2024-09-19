{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_bpa_report') }}
),
final as (
    select 
    date
,bidding_type
,sales_method
,campaign_start_date
,campaign_end_date
,ad_objectives
,campaign_name
,campaign_id
,ad_group
,ad_group_id
,ad_name
,template_type
,advertisement_id
,impression_area
,material_type
,material
,material_id
,product
,option_id
,ad_execution_product_name
,ad_execution_option_id
,landing_page_type
,landing_page_name
,landing_page_id
,impressed_keywords
,input_keywords
,keyword_extension_type
,category
,impression_count
,click_count
,ctr
,ad_cost
,total_orders_1d
,direct_orders_1d
,indirect_orders_1d
,total_sales_1d
,direct_sales_qty_1d
,indirect_sales_qty_1d
,total_conversion_sales_1d
,direct_conversion_sales_1d
,indirect_conversion_sales_1d
,total_orders_14d
,direct_orders_14d
,indirect_orders_14d
,total_sales_14d
,direct_sales_qty_14d
,indirect_sales_qty_14d
,total_conversion_sales_14d
,direct_conversion_sales_14d
,indirect_conversion_sales_14d
,total_ad_return_1d
,direct_ad_return_1d
,indirect_ad_return_1d
,total_ad_return_14d
,direct_ad_return_14d
,indirect_ad_return_14d,
null as file_name,
current_timestamp as crtd_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crtd_dttm > (select max(crtd_dttm) from {{ this }})
    {% endif %}
)
select * from final