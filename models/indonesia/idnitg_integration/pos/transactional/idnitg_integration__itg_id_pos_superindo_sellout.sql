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
    company,
    company_original,
    region,
    grp,
    product,
    mon_sales_percent,
	mon_order,
	mon_supply,
    pos_cust,
    run_id,
    filename,
    yearmonth,
    current_timestamp()::timestamp_ntz AS crtd_dttm
    from source
)
select * from final