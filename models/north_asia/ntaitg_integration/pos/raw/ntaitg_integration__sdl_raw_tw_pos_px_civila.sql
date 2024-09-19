{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_tw_pos_px_civila') }}
),
final as
(
    select 
        store_code as store_code,
        store_name_chinese as store_name_chinese,
        pos_date as pos_date,
        ean_code as ean_code,
        civilian_product_code as civilian_product_code,
        brand as brand,
        product_name as product_name,
        dc as dc,
        unit_price as unit_price,
        stock_receive_qty_by_store as stock_receive_qty_by_store,
        stock_selling_qty_by_store as stock_selling_qty_by_store,
        stock_inventory_qty as stock_inventory_qty,
        stock_return_qty_by_store as stock_return_qty_by_store,
        stock_receive_amt_by_store as stock_receive_amt_by_store,
        stock_selling_amt_by_store as stock_selling_amt_by_store,
        stock_inventory_amt as stock_inventory_amt,
        stock_return_amt_by_store as stock_return_amt_by_store,
        crt_dttm as crt_dttm,
        upd_dttm as upd_dttm,
        null as filename,
        null as run_id
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final
