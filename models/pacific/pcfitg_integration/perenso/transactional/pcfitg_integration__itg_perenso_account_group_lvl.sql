with sdl_perenso_account_group_lvl as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_group_lvl') }}
),
final as (
select
acct_grp_lev_key::number(10,0) as acct_grp_lev_key,
acct_lev_desc::varchar(100) as acct_lev_desc,
acct_lev_index::number(10,0) as acct_lev_index,
field_key::number(10,0) as field_key,
run_id::number(14,0) as run_id,
create_dt::timestamp_ntz(9) as create_dt,
current_timestamp()::timestamp_ntz(9) as update_dt
from sdl_perenso_account_group_lvl
)
select * from final