with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_rtldistributor') }}
    where filename not in (
    select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_rtldistributor__null_test') }}
    union all
    select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_rtldistributor__duplicate_test') }}
    )
),
final as 
(
    select 
	cmpcode::varchar(10) as cmpcode,
	tlcode::varchar(50) as tlcode,
	distrcode::varchar(30) as distrcode,
	dateofjoin::timestamp_ntz(9) as dateofjoin,
	isactive::varchar(1) as isactive,
	modusercode::varchar(50) as modusercode,
	moddt::timestamp_ntz(9) as moddt,
	createddt::timestamp_ntz(9) as createddt,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final