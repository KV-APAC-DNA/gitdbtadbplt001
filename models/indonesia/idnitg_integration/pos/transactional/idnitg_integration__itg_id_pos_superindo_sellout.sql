{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_superindo_sellout') }}
),
final as
(
    select
    company::varchar(100) as company,
    company_original::varchar(100) as company_original,
    region::varchar(20) as region,
    grp::varchar(50) as grp,
    product::varchar(200) as product,
    mon_sales_percent::number(10,2) as mon_sales_percent,
    mon_order::number(18,2) as mon_order,
    mon_supply::number(18,2) as mon_supply,
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final