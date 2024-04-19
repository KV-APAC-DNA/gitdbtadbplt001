{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['store_chk_hdr_key']
    )
}}
with sdl_perenso_store_chk_hdr as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_store_chk_hdr') }}
),
final as (
select
    store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
    diary_item_key::number(10,0) as diary_item_key,
    acct_key::number(10,0) as acct_key,
    work_item_key::number(10,0) as work_item_key,
    to_timestamp(store_chk_date, 'DD/MM/YYYY')::timestamp_ntz(9)::timestamp_ntz(9) as store_chk_date,
    run_id::number(14,0) as run_id,
    create_dt::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt 
from sdl_perenso_store_chk_hdr )
select * from final