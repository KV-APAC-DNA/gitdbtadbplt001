{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_watson_sellout') }}
),
final as
(
    select
    loc_idnt::varchar(5) as loc_idnt,
    sale_date::timestamp_ntz(9) as sale_date,
    str_code::varchar(50) as str_code,
    str_name::varchar(50) as str_name,
    str_class::varchar(50) as str_class,
    str_format::varchar(50) as str_format,
    division::varchar(10) as division,
    div_desc::varchar(100) as div_desc,
    dept_idnt::varchar(10) as dept_idnt,
    dept_desc::varchar(200) as dept_desc,
    sup_code::varchar(50) as sup_code,
    sup_name::varchar(100) as sup_name,
    prdt_code::varchar(50) as prdt_code,
    prdt_desc::varchar(200) as prdt_desc,
    brand::varchar(50) as brand,
    sale_qty::number(18,2) as sale_qty,
    net_sale::number(18,2) as net_sale,
    week::varchar(10) as week,
    year::varchar(4) as year,
    range_desc::varchar(100) as range_desc,
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final