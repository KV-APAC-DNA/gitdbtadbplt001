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
    NAMA_BARANG,
    qty,
    sales,
    pos_cust,
	branch,
    run_id,
    filename,
    yearmonth,
    current_timestamp()::timestamp_ntz AS crtd_dttm
    from source
)
select * from final