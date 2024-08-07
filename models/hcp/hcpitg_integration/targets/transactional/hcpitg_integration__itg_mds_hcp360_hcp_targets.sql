with sdl_mds_in_hcp_targets as(
    select * from {{ source('hcpsdl_raw', 'sdl_mds_in_hcp_targets') }}
),
final as (
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
	country::varchar(200) as country,
	brand::varchar(200) as brand,
	channel_code::varchar(500) as channel_code,
	channel_name::varchar(500) as channel_name,
	channel_id::number(18,0) as channel_id,
	activity_type_code::varchar(500) as activity_type_code,
	activity_type_name::varchar(500) as activity_type_name,
	activity_type_id::number(18,0) as activity_type_id,
	kpi_code::varchar(500) as kpi_code,
	kpi_name::varchar(500) as kpi_name,
	kpi_id::number(18,0) as kpi_id,
	year::number(31,0) as year,
	jan::varchar(200) as jan,
	feb::varchar(200) as feb,
	mar::varchar(200) as mar,
	apr::varchar(200) as apr,
	may::varchar(200) as may,
	jun::varchar(200) as jun,
	jul::varchar(200) as jul,
	aug::varchar(200) as aug,
	sep::varchar(200) as sep,
	oct::varchar(200) as oct,
	nov::varchar(200) as nov,
	dec::varchar(200) as dec,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
	enterusername::varchar(200) as enterusername,
	enterversionnumber::number(18,0) as enterversionnumber,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	validationstatus::varchar(500) as validationstatus,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
from
       sdl_mds_in_hcp_targets
)
select * from final