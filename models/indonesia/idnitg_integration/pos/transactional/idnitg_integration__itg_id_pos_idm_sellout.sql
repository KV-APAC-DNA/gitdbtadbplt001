{{
    config
    (
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_idm_sellout') }}
),

final as (
    select
        item::varchar(20) as item,
        description::varchar(200) as description,
        plu::varchar(50) as plu,
        branch::varchar(100) as branch,
        type::varchar(10) as type,
        "values"::number(18,2) as "values",
        upper(pos_cust)::varchar(50) as pos_cust,
        yearmonth::varchar(10) as yearmonth,
        run_id::number(14,0) as run_id,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crtd_dttm,
        filename::varchar(100) as filename
    from source
)

select * from final
