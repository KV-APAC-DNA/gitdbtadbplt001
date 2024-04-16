with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_survey_targets') }}
),
final as
(
select 
    year::varchar(256) as year,
	cycle_start_date::varchar(256) as cycle_start_date,
	cycle_end_date::varchar(256) as cycle_end_date,
	account_type_description::varchar(256) as account_type_description,
	target_type::varchar(256) as target_type,
	category::varchar(256) as category,
	territory_region::varchar(256) as territory_region,
	territory::varchar(256) as territory,
	target::number(20,0) as target,
    run_id::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as create_dt,
    current_timestamp()::timestamp_ntz(9) as update_dt
from source
)
select * from final