{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_diamond_sellout') }}
),
final as
(
    select
    nama_barang::varchar(200) as nama_barang,
    qty::number(18,2) as qty,
    sales::number(18,2) as sales,
    pos_cust::varchar(50) as pos_cust,
    branch::varchar(100) as branch,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final