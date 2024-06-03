{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_matahari_otc_stock') }}
),

final as (
    select
        type::varchar(50) as type,
        loc::varchar(10) as loc,
        store_name::varchar(100) as store_name,
        item::varchar(50) as item,
        item_desc::varchar(200) as item_desc,
        soh::number(18, 2) as soh,
        upper(pos_cust)::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14, 0) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
