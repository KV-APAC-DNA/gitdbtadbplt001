{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where trim(date_yyyymm)||trim(coupang_id)||trim(category_depth1)||trim(brand) in (select distinct trim(date_yyyymm)||trim(coupang_id)||trim(category_depth1) || trim(brand) from {{ source('ntasdl_raw', 'sdl_kr_coupang_customer_brand_trend') }});
        {% endif %}"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_customer_brand_trend') }}
),
final as (
    SELECT 
        date_yyyymm::varchar(6) as date_yyyymm,
        coupang_id::varchar(15) as coupang_id,
        category_depth1::varchar(30) as category_depth1,
        brand::varchar(30) as brand,
        new_user_count::varchar(10) as new_user_count,
        curr_user_count::varchar(10) as curr_user_count,
        tot_user_count::varchar(10) as tot_user_count,
        new_user_sales_amt::number(15,2) as new_user_sales_amt,
        curr_user_sales_amt::number(15,2) as curr_user_sales_amt,
        new_user_avg_product_sales_price::number(10,2) as new_user_avg_product_sales_price,
        curr_user_avg_product_sales_price::number(10,2) as curr_user_avg_product_sales_price,
        tot_user_avg_product_sales_price::number(10,2) as tot_user_avg_product_sales_price,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        yearmo::varchar(20) as yearmo,
        data_granularity::varchar(10) as data_granularity,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final