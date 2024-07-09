
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where trim(transaction_date)||trim(sku_id)||trim(barcode)||trim(sku_people) in (select distinct trim(transaction_date)||trim(sku_id)||trim(barcode)||trim(sku_people) from {{ source('ntasdl_raw', 'sdl_kr_coupang_productsalereport') }});
        {% endif %}"         
)
}}


with sdl_kr_coupang_productsalereport as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_productsalereport') }}
),
final as 
(
    SELECT 	
        'KR'::varchar(255) as country_cd,
        transaction_date::date as transaction_date,
        sku_id::varchar(255) as sku_id,
        sku_people::varchar(255) as sku_people,
        vendor_item_id::varchar(255) as vendor_item_id,
        vendor_item::varchar(255) as vendor_item,
        product_id::varchar(255) as product_id,
        barcode::varchar(255) as barcode,
        product_category_high::varchar(255) as product_category_high,
        product_category_mid::varchar(255) as product_category_mid,
        product_category_low::varchar(255) as product_category_low,
        brand::varchar(255) as brand,
        sales_gmv::number(18,6) as sales_gmv,
        cost_of_purchase::number(18,6) as cost_of_purchase,
        units_sold::number(18,0) as units_sold,
        shipping_sales_gmv::varchar(255) as shipping_sales_gmv,
        shipping_weight_percent::varchar(255) as shipping_weight_percent,
        sns_cogs::varchar(255) as sns_cogs,
        sns_units_sold::varchar(255) as sns_units_sold,
        no_of_product_reviews::varchar(255) as no_of_product_reviews,
        avg_product_rating::varchar(255) as avg_product_rating,
        'KRW'::varchar(255) as transaction_currency,
        run_id::number(14,0) as run_id,
        file_name::varchar(255) as file_name,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    FROM sdl_kr_coupang_productsalereport
)
select * from final