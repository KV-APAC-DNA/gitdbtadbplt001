with sdl_perenso_account_fields as (
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_account_fields') }}
    -- select * from DEV_DNA_LOAD.SNAPPCFSDL_RAW.sdl_perenso_account_fields
),
final as (
select
field_key::number(10,0) as field_key,
field_desc::varchar(100) as field_desc,
field_type::number(10,0) as field_type,
acct_type_key::number(10,0) as acct_type_key,
active::varchar(10) as active,
run_id::number(14,0) as run_id,
create_dt::timestamp_ntz(9) as create_dt,
current_timestamp()::timestamp_ntz(9) as update_dt
from sdl_perenso_account_fields
)
select * from final