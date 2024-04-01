{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_th_tesco_transdata') }}
),
final as
(
    select 
        creation_date,
        supplier_id,
        supplier_name,
        warehouse,
        delivery_point_name,
        ir_date,
        eansku,
        article_id,
        spn,
        article_name,
        stock,
        sales,
        sales_amount,
        file_name,
        folder_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
        from source
)
select * from final