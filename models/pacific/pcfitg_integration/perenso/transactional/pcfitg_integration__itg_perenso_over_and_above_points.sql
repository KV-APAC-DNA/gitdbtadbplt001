with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_over_and_above_points') }}
),
final as
(
select 
    display_type,
    points,
    run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final