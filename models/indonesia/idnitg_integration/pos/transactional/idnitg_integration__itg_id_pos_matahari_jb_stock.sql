{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_matahari_jb_stock') }}
),

final as (
    select
        sku::varchar(50) as sku,
        sku_desc::varchar(200) as sku_desc,
        qty::number(18, 2) as qty,
        retail_values::number(18, 2) as retail_values,
        upper(pos_cust)::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14, 0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
