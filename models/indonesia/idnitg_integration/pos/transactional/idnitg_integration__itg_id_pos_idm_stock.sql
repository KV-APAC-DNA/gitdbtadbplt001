{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_idm_stock') }}
),

final as (
    select
        no::number(20, 0) as no,
        item::varchar(200) as item,
        branch::varchar(50) as branch,
        dc_qty::number(18, 2) as dc_qty,
        store_qty::number(18, 2) as store_qty,
        units::varchar(10) as units,
        upper(pos_cust)::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14, 0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
