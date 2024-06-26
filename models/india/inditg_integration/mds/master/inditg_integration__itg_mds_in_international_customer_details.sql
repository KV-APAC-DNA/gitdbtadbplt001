with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_international_customer_details') }}
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
	region_name_code::varchar(500) as region_name_code,
	region_name_name::varchar(500) as region_name_name,
	region_name_id::number(18,0) as region_name_id,
	zone_name_code::varchar(500) as zone_name_code,
	zone_name_name::varchar(500) as zone_name_name,
	zone_name_id::number(18,0) as zone_name_id,
	territory_name_code::varchar(500) as territory_name_code,
	territory_name_name::varchar(500) as territory_name_name,
	territory_name_id::number(18,0) as territory_name_id,
	distributor_type_code::varchar(500) as distributor_type_code,
	distributor_type_name::varchar(500) as distributor_type_name,
	distributor_type_id::number(18,0) as distributor_type_id,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
	enterusername::varchar(200) as enterusername,
	enterversionnumber::number(18,0) as enterversionnumber,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	validationstatus::varchar(500) as validationstatus,
	convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final