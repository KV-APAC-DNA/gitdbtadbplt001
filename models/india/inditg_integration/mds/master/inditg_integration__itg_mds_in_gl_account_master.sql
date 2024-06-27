with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_gl_account_master') }}
),
final as 
(
    select 
    id::number(18,0) as id,
	muid::varchar(36) as muid,
	versionname::varchar(100) as versionname,
	versionnumber::number(18,0) as versionnumber,
	version_id::number(18,0) as version_id,
	versionflag::varchar(100) as versionflag,
	name::varchar(500) as name,
	code::varchar(500) as code,
	changetrackingmask::number(18,0) as changetrackingmask,
	bw_gl::varchar(200) as bw_gl,
	nature_code::varchar(500) as nature_code,
	nature_name::varchar(500) as nature_name,
	nature_id::number(18,0) as nature_id,
	bravo_mapping_code::varchar(500) as bravo_mapping_code,
	bravo_mapping_name::varchar(500) as bravo_mapping_name,
	bravo_mapping_id::number(18,0) as bravo_mapping_id,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
	enterusername::varchar(200) as enterusername,
	enterversionnumber::number(18,0) as enterversionnumber,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	validationstatus::varchar(500) as validationstatus,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dt
    from source
)
select * from final