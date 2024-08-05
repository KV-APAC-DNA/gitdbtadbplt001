with sdl_prox_sys_department as
(
    select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.SDL_PROX_SYS_DEPARTMENT
    --select * from {{source('aspsdl_raw', 'sdl_prox_sys_department')}}
),
final as
(
    select
    deptid::varchar(50) as deptid,
	deptcode::varchar(50) as deptcode,
	deptname::varchar(255) as deptname,
	deptnameen::varchar(255) as deptnameen,
	parentid::varchar(50) as parentid,
	levelid::number(38,0) as levelid,
	sortid::varchar(100) as sortid,
	status::varchar(100) as status,
	applicationid::varchar(50) as applicationid,
	col1::varchar(255) as col1,
	col2::varchar(255) as col2,
	col3::varchar(255) as col3,
	version::varchar(50) as version,
	orgpath::varchar(100) as orgpath,
	orgtype::varchar(50) as orgtype,
	costcenter::varchar(50) as costcenter,
	costcentername::varchar(100) as costcentername,
	locationcode::varchar(100) as locationcode,
	orgcode::varchar(255) as orgcode,
	createuserid::varchar(50) as createuserid,
	createtime::timestamp_ntz(9) as createtime,
	lastmodifyuserid::varchar(50) as lastmodifyuserid,
	lastmodifytime::timestamp_ntz(9) as lastmodifytime,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	crt_dttm::timestamp_ntz(9) as crt_dttm
    from sdl_prox_sys_department
)
select * from final