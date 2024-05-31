{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_guardian_stock') }}
),

final as (
    select
        article::varchar(50) as article,
        article_desc::varchar(200) as article_desc,
        category::varchar(50) as category,
        soh_stores::number(18, 2) as soh_stores,
        soh_dc::number(18, 2) as soh_dc,
        pos_cust::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14, 0) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
