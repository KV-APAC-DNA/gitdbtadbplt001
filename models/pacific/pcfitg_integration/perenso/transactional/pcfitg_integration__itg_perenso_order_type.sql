with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_order_type') }}
),
final as 
(
    select 
    order_type_key::number(10,0) as order_type_key,
	order_type_desc::varchar(255) as order_type_desc,
	source::number(10,0) as source,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final