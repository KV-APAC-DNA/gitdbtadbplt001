with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_in_rtlsalesman') }}
    where filename not in (
        select distinct filename from {{source('indwks_integration','TRATBL_sdl_in_rtlsalesman__null_test')}}
        union all
        select distinct filename from {{source('indwks_integration','TRATBL_sdl_in_rtlsalesman__duplicate_test')}})

),
final as 
(
    select 
	cmpcode::varchar(10) cmpcode,
	tlcode::varchar(50) tlcode,
	distrcode::varchar(30) distrcode,
	distrbrcode::varchar(30) distrbrcode,
	salesmancode::varchar(50) salesmancode,
	dateofjoin::timestamp_ntz(9) dateofjoin,
	isactive::varchar(1) isactive,
	modusercode::varchar(50) modusercode,
	moddt::timestamp_ntz(9) moddt,
	createddt::timestamp_ntz(9) createddt,
	filename::varchar(100) filename,
	run_id::varchar(50) run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final