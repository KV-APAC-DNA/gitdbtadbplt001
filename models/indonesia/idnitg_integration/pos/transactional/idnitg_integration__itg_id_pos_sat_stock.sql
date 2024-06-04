{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_sat_stock') }}
),
final as
(
    select
    no::varchar(10) as no,
    plu::varchar(50) as plu,
    description::varchar(200) as description,
    branch::varchar(50) as branch,
    store_dc::varchar(50) as store_dc,
    type::varchar(10) as type,
    "values"::number(19,2) as "values",
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final