with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_constants') }}
),
final as 
(
select 
    const_key::number(10,0) as const_key,
	const_desc::varchar(255) as const_desc,
	const_type::number(10,0) as const_type,
	dsporder::number(10,0) as dsporder,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
from source
)
select * from final