with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_rretailergeoextension') }}
),
final as 
(
    select 
	cmpcode::varchar(10) as cmpcode,
	distrcode::varchar(50) as distrcode,
	customercode::varchar(50) as customercode,
	cmpcutomercode::varchar(50) as cmpcutomercode,
	distributorcustomercode::varchar(50) as distributorcustomercode,
	latitude::varchar(20) as latitude,
	longitude::varchar(20) as longitude,
	townname::varchar(100) as townname,
	statename::varchar(100) as statename,
	districtname::varchar(100) as districtname,
	subdistrictname::varchar(100) as subdistrictname,
	type::varchar(10) as type,
	villagename::varchar(100) as villagename,
	pincode::number(18,0) as pincode,
	uacheck::varchar(100) as uacheck,
	uaname::varchar(100) as uaname,
	population::number(18,0) as population,
	popstrata::varchar(100) as popstrata,
	finalpopulationwithua::number(18,0) as finalpopulationwithua,
	modifydate::timestamp_ntz(9) as modifydate,
	createddate::timestamp_ntz(9) as createddate,
	isdeleted::varchar(1) as isdeleted,
	extrafield1::varchar(100) as extrafield1,
	extrafield2::varchar(100) as extrafield2,
	extrafield3::varchar(100) as extrafield3,
	createddt::timestamp_ntz(9) as createddt,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final