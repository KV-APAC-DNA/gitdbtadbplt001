with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_deal_discount') }}
),
final as 
(
    select 
    deal_key::number(10,0) as deal_key,
	deal_desc::varchar(255) as deal_desc,
	to_date(to_varchar(to_date(start_date,'dd/mm/yyyy'),'yyyy-mm-dd'),'yyyy-mm-dd') as start_date,
    to_date(to_varchar(to_date(end_date,'dd/mm/yyyy'),'yyyy-mm-dd'),'yyyy-mm-dd') as end_date,
	short_desc::varchar(255) as short_desc,
	disc_key::number(10,0) as disc_key,
	discount_desc::varchar(255) as discount_desc,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final