{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where report_date in  (select distinct report_date from {{ source('ntasdl_raw', 'sdl_kr_dads_coupang_price') }});
        {% endif %}"
)
}}
with sdl_kr_dads_coupang_price as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_dads_coupang_price') }}
),
final as (
    SELECT 
        report_date::varchar(100) as report_date,
        trusted_upc::varchar(100) as trusted_upc,
        trusted_rpc::varchar(100) as trusted_rpc,
        trusted_mpc::varchar(100) as trusted_mpc,
        trusted_product_description::varchar(100) as trusted_product_description,
        region::varchar(100) as region,
        online_store::varchar(100) as online_store,
        brand::varchar(100) as brand,
        manufacturer::varchar(100) as manufacturer,
        category::varchar(100) as category,
        dimension1::varchar(100) as dimension1,
        sub_category::varchar(100) as sub_category,
        brand_subcategory::varchar(100) as brand_subcategory,
        dimension4::varchar(100) as dimension4,
        dimension5::varchar(100) as dimension5,
        dimension6::varchar(100) as dimension6,
        seller::varchar(100) as seller,
        power_sku::varchar(100) as power_sku,
        availability_status::varchar(100) as availability_status,
        currency::varchar(100) as currency,
        observed_price::varchar(100) as observed_price,
        store_list_price::varchar(100) as store_list_price,
        min_price::varchar(100) as min_price,
        max_price::varchar(100) as max_price,
        min_max_diff_pct::varchar(100) as min_max_diff_pct,
        min_max_diff_price::varchar(100) as min_max_diff_price,
        msrp::varchar(100) as msrp,
        msrp_diff_pct::varchar(100) as msrp_diff_pct,
        msrp_diff_amount::varchar(100) as msrp_diff_amount,
        previous_day_price::varchar(100) as previous_day_price,
        previous_day_diff_pct::varchar(100) as previous_day_diff_pct,
        previous_day_diff_amount::varchar(100) as previous_day_diff_amount,
        promotion_text::varchar(100) as promotion_text,
        url::varchar(100) as url,
        file_name::varchar(255) as file_name  
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM sdl_kr_dads_coupang_price
)
select * from final