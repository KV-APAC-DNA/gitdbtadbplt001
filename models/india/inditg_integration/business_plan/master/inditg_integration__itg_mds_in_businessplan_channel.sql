with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_businessplan_channel') }}
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
	region_code::varchar(500) as region_code,
	region_name::varchar(500) as region_name,
	region_id::number(18,0) as region_id,
	gsm_code::varchar(500) as gsm_code,
	gsm_name::varchar(500) as gsm_name,
	gsm_id::number(18,0) as gsm_id,
	cluster_code::varchar(500) as cluster_code,
	cluster_name::varchar(500) as cluster_name,
	cluster_id::number(18,0) as cluster_id,
	channel_name_code::varchar(500) as channel_name_code,
	channel_name_name::varchar(500) as channel_name_name,
	channel_name_id::number(18,0) as channel_name_id,
	plan::number(31,0) as plan,
	year_code::varchar(500) as year_code,
	year_name::varchar(500) as year_name,
	year_id::number(18,0) as year_id,
	month_code::varchar(500) as month_code,
	month_name::varchar(500) as month_name,
	month_id::number(18,0) as month_id,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
	enterusername::varchar(200) as enterusername,
	enterversionnumber::number(18,0) as enterversionnumber,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	validationstatus::varchar(500) as validationstatus
    from source
)
select  * from final