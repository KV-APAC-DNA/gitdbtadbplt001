{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['partner_gln', 'supplier_code', 'inventory_date', 'barcode','inventory_location']
    )
}}

with source as(
    select *, dense_rank() over(partition by partner_gln, supplier_code, inventory_report_date, ean_item_code ,inventory_location order by filename desc) as rnk 
    from {{ source('thasdl_raw','sdl_th_mt_7_11') }}
    where filename not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_7_11__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_mt_7_11__duplicate_test') }}
    ) qualify rnk =1

),
final as(
    select
        partner_gln::varchar(20) as partner_gln,
        supplier_code::varchar(20) as supplier_code,
        try_to_date(inventory_report_date, 'YYYYMMDD') - 1 as inventory_date,
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
        current_timestamp()::timestamp_ntz(9) AS updt_dttm
    from source
)
select * from final