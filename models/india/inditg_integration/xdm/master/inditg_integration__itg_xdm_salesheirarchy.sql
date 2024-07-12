with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_salesheirarchy') }}
),
final as 
(
    select 
    rsmcode::varchar(50) as rsmcode,
	rsmname::varchar(100) as rsmname,
	rsm_flmasmcode::varchar(50) as rsm_flmasmcode,
	flmasmcode::varchar(50) as flmasmcode,
	flmasmname::varchar(100) as flmasmname,
	flmasm_abicode::varchar(50) as flmasm_abicode,
	abicode::varchar(50) as abicode,
	abiname::varchar(100) as abiname,
	abi_distributorcode::varchar(50) as abi_distributorcode,
	distributorcode::varchar(50) as distributorcode,
	distributorname::varchar(100) as distributorname,
	createddt::timestamp_ntz(9) as createddt,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final