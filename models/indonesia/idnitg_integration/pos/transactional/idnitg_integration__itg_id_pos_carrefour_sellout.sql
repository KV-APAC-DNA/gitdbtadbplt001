{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_carrefour_sellout') }}
),
final as
(
    select
    fdesc::varchar(100) as fdesc,
    scc::varchar(50) as scc,
    scc_name::varchar(200) as scc_name,
    sales_qty::number(18,2) as sales_qty,
    net_sales::number(18,2) as net_sales,
    share::number(10,2) as share,
    pos_cust::varchar(50) as pos_cust,
    yearmonth::varchar(10) as yearmonth,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz as crtd_dttm,
    filename::varchar(100) as filename
    from source
)
select * from final