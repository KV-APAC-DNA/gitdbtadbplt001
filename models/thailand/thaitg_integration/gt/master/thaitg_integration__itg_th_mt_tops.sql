{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['partner_gln', 'supplier_gln', 'inventory_date', 'barcode','inventory_location']
    )
}}


with source as(
    select * from {{ source('thasdl_raw','sdl_th_mt_tops') }}
),
final as(
    select
        partner_gln::varchar(20) as partner_gln,
        supplier_gln::varchar(20) as supplier_gln,
        (
            TO_DATE(inventory_report_date, 'YYYYMMDD') - 1
        ) AS inventory_date,
        ean_item_code::varchar(20) AS barcode,
        inventory_location::varchar(20) as inventory_location,
        unit_of_measure::varchar(30) as unit_of_measure,
        qty_per_pack::number(10,0) as qty_per_pack,
        total_qty_onhand::number(15,0) as total_qty_onhand,
        actual_onhand_stock_qty::number(15,0) as actual_onhand_stock_qty,
        qty_in_transit::number(15,0) as qty_in_transit,
        sales_qty::number(15,0) as sales_qty,
        expected_sales_qty::number(15,0) as expected_sales_qty,
        short_shipped_qty::number(15,0) as short_shipped_qty,
        item_price_type::varchar(20) as item_price_type,
        item_price::number(15,3) as item_price,
        item_price_unit::varchar(20) as item_price_unit,
        price_currency::varchar(10) as price_currency,
        filename::varchar(100) as filename,
        run_id::varchar(14) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final