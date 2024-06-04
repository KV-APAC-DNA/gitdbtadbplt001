{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_carrefour_stock') }}
),
final as
(
    select
    dep_desc::varchar(50) as dep_desc,
    stock_qty::number(18,2) as stock_qty,
    stock_amt::number(18,2) as stock_amt,
    stock_days::number(10,4) as stock_days,
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final