{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "delete from {{this}} 
                    where 
                    source_file_name = (
                                            select distinct source_file_name 
                                            from dev_dna_load.snapntasdl_raw.sdl_kr_ecommerce_offtake_coupang_transaction
                                        )"
    )
}}


with source as
(
    --select * from {{source('ntasdl_raw', 'sdl_kr_ecommerce_offtake_coupang_transaction')}} add to prehook as well
    select * from dev_dna_load.snapntasdl_raw.sdl_kr_ecommerce_offtake_coupang_transaction
),

trans as
(
    select 
        load_date, 
        source_file_name, 
        transaction_date :: date as transaction_date, 
        product_id, 
        barcode, 
        skuid, 
        vendor_item_id, 
        sku_name, 
        vendor_item_name, 
        brand, 
        cast(
            replace(cogs, ',', '') as numeric(18, 6)
        ) as sales_value, 
        cast(
            replace(reg_dlvry_gmv, ',', '') as numeric(18, 6)
        ) as regular_shipping_sales_sns_gmv, 
        0 as regular_shipping_weight, 
        cast(
            replace(cogs, ',', '') as numeric(18, 6)
        ) as purchase_cost_cogs, 
        0 as periodic_shipping_cost_sns_cogs, 
        cast(
            replace(units_sold, ',', '') as numeric
        ) as quantity, 
        cast(
            replace(reg_dlvry_units_sold, ',', '') as numeric
        ) as regular_shippment_sns_units_sold, 
        cast(
            replace(review_count, ',', '') as numeric(18, 6)
        ) as reviews, 
        cast(
            replace(avg_product_review_rate, ',', '') as numeric(18, 6)
        ) as average_product_rating, 
        cast(
            replace(ordered_customer_count, ',', '') as numeric
        ) as active_customers, 
        0 as new_customers, 
        'KRW' as transaction_currency, 
        'Korea' as country 
    from source

),

final as 
(
    select 
        load_date::timestamp_ntz(9) as load_date,
        source_file_name::varchar(255) as source_file_name,
        transaction_date::timestamp_ntz(9) as transaction_date,
        product_id::varchar(20) as product_id,
        barcode::varchar(255) as barcode,
        skuid::varchar(20) as sku_code,
        vendor_item_id::varchar(255) as vendor_item_id,
        sku_name::varchar(20000) as sku_name,
        vendor_item_name::varchar(20000) as vendor_item_name,
        brand::varchar(255) as brand,
        sales_value::float as sales_value,
        regular_shipping_sales_sns_gmv::float as regular_shipping_sales_sns_gmv,
        regular_shipping_weight::float as regular_shipping_weight,
        purchase_cost_cogs::float as purchase_cost_cogs,
        periodic_shipping_cost_sns_cogs::float as periodic_shipping_cost_sns_cogs,
        quantity::float as quantity,
        regular_shippment_sns_units_sold::float as regular_shippment_sns_units_sold,
        reviews::float as reviews,
        average_product_rating::float as average_product_rating,
        active_customers::number(18,0) as active_customers,
        new_customers::number(18,0) as new_customers,
        transaction_currency::varchar(10) as transaction_currency,
        country::varchar(10) as country
    from trans
)
select * from final