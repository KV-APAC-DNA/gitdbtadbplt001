{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['store_chk_hdr_key','prod_grp_key','todo_key']
    )
}}
with sdl_perenso_head_office_req_check as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_head_office_req_check') }}
),
final as (
select 
    store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
    todo_key::number(10,0) as todo_key,
    prod_grp_key::number(10,0) as prod_grp_key,
    notes::varchar(600) as notes,
    fail_reason_key::varchar(256) as fail_reason_key,
    fail_reason_desc::varchar(256) as fail_reason_desc,
    run_id::number(14,0) as run_id,
    create_dt::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from sdl_perenso_head_office_req_check 
 )
select * from final