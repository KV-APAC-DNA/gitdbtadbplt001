with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_survey_targets') }}
),
final as
(
select 
    year,
    cycle_start_date,
    cycle_end_date,
    account_type_description,
    target_type,
    category,
    territory_region,
    territory,
    target,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final