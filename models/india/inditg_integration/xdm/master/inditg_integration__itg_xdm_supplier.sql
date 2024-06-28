with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_supplier') }}
),
final as 
(
    select 
    cmpcode::varchar(10) as cmpcode,
	supcode::varchar(30) as supcode,
	suptype::varchar(1) as suptype,
	supname::varchar(100) as supname,
	supaddr1::varchar(50) as supaddr1,
	supaddr2::varchar(50) as supaddr2,
	supaddr3::varchar(50) as supaddr3,
	city::varchar(50) as city,
	state::varchar(50) as state,
	country::varchar(50) as country,
	gststatecode::varchar(30) as gststatecode,
	suppliergstin::varchar(15) as suppliergstin,
	createddt::timestamp_ntz(9) as createddt,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final