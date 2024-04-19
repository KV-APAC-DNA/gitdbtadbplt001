{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['store_chk_hdr_key','prod_grp_key','todo_key']
    )
}}
with sdl_perenso_head_office_req_state as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_head_office_req_state') }}
),
final as (
select
    acct_key::number(10,0) as acct_key,
    todo_key::number(10,0) as todo_key,
    prod_grp_key::number(10,0) as prod_grp_key,
    to_timestamp(start_date, 'DD/MM/YYYY')::timestamp_ntz(9) as start_date,
    to_timestamp(end_date, 'DD/MM/YYYY')::timestamp_ntz(9) as end_date,
    req_state::number(10,0) as req_state,
    store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
    run_id::number(14,0) as run_id,
    create_dt::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt 
from sdl_perenso_head_office_req_state )
select * from final