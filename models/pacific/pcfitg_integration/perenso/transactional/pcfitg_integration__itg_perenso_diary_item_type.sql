with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_diary_item_type') }}
),
final as
(
    select 
    diary_item_type_key::number(14,0) as diary_item_type_key ,
	diary_item_type_desc::varchar(255) as diary_item_type_desc,
	dsp_order::number(14,0) as dsp_order,
	active::varchar(5) as active,
	category::number(14,0) as category,
	count_in_call_rate::varchar(5) as count_in_call_rate,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt ,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final