{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['filename'],
        pre_hook= ["delete from {{this}} where substring(filename, 15, 6) in ( select substring(filename, 15, 6) from {{ source('phlsdl_raw', 'sdl_ph_ecommerce_offtake_acommerce') }} );"]
    )
}}

with source as(
    select * from {{ source('phlsdl_raw', 'sdl_ph_ecommerce_offtake_acommerce') }}
),
final as(
    select 
        partner_id::number(18,0) as partner_id,
        prefix::varchar(10) as prefix,
        client::varchar(100) as client,
        order_date::timestamp_ntz(9) as order_date,
        month_no::number(18,0) as month_no,
        month::varchar(10) as month,
        day::number(18,0) as day,
        marketplace_order_date::timestamp_ntz(9) as marketplace_order_date,
        marketplace_month::varchar(12) as marketplace_month,
        marketplace_day::number(18,0) as marketplace_day,
        delivered_date::timestamp_ntz(9) as delivered_date,
        order_id::varchar(30) as order_id,
        partner_order_id::varchar(30) as partner_order_id,
        delivery_status::varchar(30) as delivery_status,
        item_sku::varchar(20) as item_sku,
        acommerce_item_sku::varchar(20) as acommerce_item_sku,
        sub_sales_channel::varchar(50) as sub_sales_channel,
        payment_type::varchar(10) as payment_type,
        product_title::varchar(400) as product_title,
        brand::varchar(50) as brand,
        mapping::varchar(50) as mapping,
        ltp::number(23,5) as ltp,
        jj_srp::number(23,5) as jj_srp,
        margin::varchar(10) as margin,
        type::varchar(20) as type,
        quantity::number(18,0) as quantity,
        gmv::number(23,5) as gmv,
        count_order::varchar(10) as count_order,
        addressee::varchar(50) as addressee,
        shipping_province::varchar(30) as shipping_province,
        filename::varchar(255) as filename,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        null::number(14,0) as run_id
    from source
)

select * from final