with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_todo_option') }}
),
final as 
(
    select
    option_key::number(10,0) as option_key,
	todo_key::number(10,0) as todo_key,
	option_desc::varchar(256) as option_desc,
	dsp_order::number(14,0) as dsp_order,
	active::varchar(5) as active,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt,
    cascade_next_todo_key::number(10,0) as cascade_next_todo_key,
	cascadeon_answermode::number(10,0) as cascadeon_answermode 
    from source
)
select * from final