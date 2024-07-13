with itg_mds_in_sv_winculum_master_wrk as
(
    select * from {{ ref('inditg_integration__itg_mds_in_sv_winculum_master_wrk') }}
) ,
final as 
(
    select 
	customercode::varchar(200) as customercode,
	customername::varchar(200) as customername,
	regioncode::varchar(200) as regioncode,
	zonecode::varchar(200) as zonecode,
	territorycode::varchar(200) as territorycode,
	statecode::varchar(200) as statecode,
	towncode::varchar(200) as towncode,
	psnonps::varchar(200) as psnonps,
	isactive::varchar(200) as isactive,
	wholesalercode::varchar(200) as wholesalercode,
	parentcustomercode::varchar(200) as parentcustomercode,
	isdirectacct::varchar(200) as isdirectacct,
	abicode::varchar(200) as abicode,
	distributorsapid::varchar(200) as distributorsapid,
	name::varchar(500) as name,
	code::varchar(500) as code,
	changetrackingmask::number(18,0) as changetrackingmask,
	validationstatus::varchar(500) as validationstatus,
	version_id::number(18,0) as version_id,
	versionflag::varchar(100) as versionflag,
	versionname::varchar(100) as versionname,
	versionnumber::number(18,0) as versionnumber,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	effective_from::timestamp_ntz(9) as effective_from,
	effective_to::timestamp_ntz(9) as effective_to,
	active::varchar(2) as active,
	run_id::number(18,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
    from itg_mds_in_sv_winculum_master_wrk
)
select * from final

