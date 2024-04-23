with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_pacific_perenso_call_coverage_targets') }}
),
final as
(
select 
    account_type_description::varchar(256) as account_type_description,
    account_grade::varchar(50) as account_grade,
    weekly_targets::number(10,0) as weekly_targets,
    monthly_targets::number(10,0) as monthly_targets,
    yearly_targets::number(10,0) as yearly_targets,
    call_per_week::number(10,5) as call_per_week,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final