{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook = "delete from {{this}} 
                    where 
                    source_file_name = (
                                            select distinct source_file_name 
                                            from dev_dna_load.snapntasdl_raw.sdl_kr_ecommerce_offtake_sales_ebay
                                        )"
    )
}}



with source as
(
    --select * from {{source('ntasdl_raw', 'sdl_kr_ecommerce_offtake_sales_ebay')}} add to prehook as well
    select * from dev_dna_load.snapntasdl_raw.sdl_kr_ecommerce_offtake_sales_ebay
),

trans as
(
    select 
        load_date, 
        source_file_name, 
        case when sku_code like '%/%' then left(
            sku_code, 
            charindex('/', sku_code)-1
        ) else sku_code end as sku_code, 
        case when sku_code like '%/%' then cast(
            replace(
            substring(
                sku_code, 
                charindex('/', sku_code)+ 1, 
                length(sku_code)
            ), 
            '개', 
            ''
            ) as integer
        ) else null end as no_of_units_sold, 
        order_date, 
        case when sales_value like '%,%' 
        and sales_value <> '' then cast(
            replace(sales_value, ',', '') as float8
        ) when sales_value not like '%,%' 
        and sales_value <> '' then cast(sales_value as float8) else NULL end as sales_value, 
        option, 
        quantity, 
        retailer_code, 
        NULL AS retailer_name, 
        transaction_date, 
        order_number, 
        product_code, 
        case when price_per_package like '%,%' 
        and price_per_package <> '' then cast(
            replace(price_per_package, ',', '') as float8
        ) when price_per_package not like '%,%' 
        and price_per_package <> '' then cast(price_per_package as float8) else NULL end as price_per_package, 
        product_title, 
        affiliates, 
        case when delivery_cost like '%,%' 
        and delivery_cost <> '' then cast(
            replace(delivery_cost, ',', '') as float8
        ) when delivery_cost not like '%,%' 
        and delivery_cost <> '' then cast(delivery_cost as float8) else NULL end as delivery_cost, 
        payment_number, 
        'KRW' as transaction_currency, 
        'Korea' as country 
    from source
),

final as
(
    select
        load_date::timestamp_ntz(9) as load_date,
        source_file_name::varchar(255) as source_file_name,
        sku_code::varchar(20) as sku_code,
        no_of_units_sold::number(18,0) as no_of_units_sold,
        order_date::timestamp_ntz(9) as order_date,
        sales_value::float as sales_value,
        option::varchar(20) as option,
        quantity::number(18,0) as quantity,
        retailer_code::varchar(2000) as retailer_code,
        retailer_name::varchar(2000) as retailer_name,
        transaction_date::timestamp_ntz(9) as transaction_date,
        order_number::varchar(255) as order_number,
        product_code::varchar(255) as product_code,
        price_per_package::float as price_per_package,
        product_title::varchar(2000) as product_title,
        affiliates::varchar(255) as affiliates,
        delivery_cost::float as delivery_cost,
        payment_number::varchar(2000) as payment_number,
        transaction_currency::varchar(10) as transaction_currency,
        country::varchar(10) as country
    from trans    
)
select * from final