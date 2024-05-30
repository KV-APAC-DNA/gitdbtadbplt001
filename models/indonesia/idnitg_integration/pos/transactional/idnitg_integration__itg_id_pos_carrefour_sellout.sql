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
    fdesc,
    scc,
    scc_name,
    sales_qty,
    net_sales,
    Share,
    pos_cust,
    yearmonth,
    run_id,
    current_timestamp()::timestamp_ntz AS crtd_dttm,
    filename
    from source
)
select * from final