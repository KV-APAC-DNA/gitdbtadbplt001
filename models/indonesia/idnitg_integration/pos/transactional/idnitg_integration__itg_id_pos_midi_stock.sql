{{
    config(
        materialized='incremental',
        incremental_strategy= "delete+insert",
        unique_key= ["filename"]
    )
}}

with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_id_pos_midi_stock') }}
),
final as
(
    select
    no,
    plu,
    description,
    branch,
    store_dc,
	type,
    "values",
    pos_cust,
    run_id,
    filename,
    yearmonth,
    current_timestamp()::timestamp_ntz AS crtd_dttm
    from source
)
select * from final