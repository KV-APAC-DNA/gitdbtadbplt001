{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_superindo_stock') }}
),
final as
(
    select
    company::varchar(100) as company,
    code::varchar(20) as code,
    description::varchar(200) as description,
    tag1::varchar(10) as tag1,
    cnv::varchar(10) as cnv,
    eq::varchar(10) as eq,
    stock_dc_regular_qty::number(18,2) as stock_dc_regular_qty,
    stock_dc_regular_dsi::number(18,2) as stock_dc_regular_dsi,
    stock_all_stores_qty::number(18,2) as stock_all_stores_qty,
    stock_all_stores_dsi::number(18,2) as stock_all_stores_dsi,
    stok_all::number(18,2) as stok_all,
    day_sales::number(18,2) as day_sales,
    dsi::number(18,2) as dsi,
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final