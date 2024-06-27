-- REQ000000180060: SSR O&A display points content change
with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_over_and_above_points') }}
),
final as
(
select 
    display_type::varchar(256) as oa_display_type,
    points::number(10,3) as points,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final