{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw','sdl_kr_pos_emart_line') }}
),
final as(
    select 
        mesg_no,
        line_no,
        store_code,
        store_name,
        sale_date,
        sale_date_form,
        sku_code,
        instore_code,
        sku_name1,
        sku_name2,
        day_sale_amnt,
        mnth_sale_amnt,
        unit_prce,
        day_sale_qty,
        qty_unit,
        mnth_sale_qty,
        convert_timezone('UTC', current_timestamp()) as crt_dttm, 
        convert_timezone('UTC', current_timestamp()) as updt_dttm 
    from source
)

select * from final