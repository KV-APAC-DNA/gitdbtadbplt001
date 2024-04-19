with sdl_perenso_account_group as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_group') }}
),
final as (
select
acct_grp_key::number(10,0) as acct_grp_key,
acct_grp_lev_key::number(10,0) as acct_grp_lev_key,
grp_desc::varchar(256) as grp_desc,
dsp_order::number(10,0) as dsp_order,
parent_key::number(10,0) as parent_key,
run_id::number(14,0) as run_id,
create_dt::timestamp_ntz(9) as create_dt,
current_timestamp()::timestamp_ntz(9) as update_dt
from sdl_perenso_account_group
)
select * from final