--- select * from {{ source('pcfsdl_raw', 'sdl_perenso_users') }}


with sdl_perenso_users as (
    select * from DEV_DNA_LOAD.SNAPPCFSDL_RAW.SDL_PERENSO_USERS
),

final as (
select 
    user_key::number(10,0) as user_key,
    display_name::varchar(100) as display_name,
    user_type_key::number(10,0) as user_type_key,
    user_type_desc::varchar(100) as user_type_desc,
    active::varchar(5) as active,
    email_address::varchar(100) as email_address,
    run_id::number(14,0) as run_id,
    create_dt::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from sdl_perenso_users
)
select * from final