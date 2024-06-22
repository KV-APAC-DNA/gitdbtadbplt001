with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_product_hierarchy') }}
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
	brand_combi_code::varchar(500) as brand_combi_code,
	brand_combi_name::varchar(500) as brand_combi_name,
	brand_combi_id::number(18,0) as brand_combi_id,
	brand_combi_var_code::varchar(200) as brand_combi_var_code,
	franchise_code::varchar(500) as franchise_code,
	franchise_name::varchar(500) as franchise_name,
	franchise_id::number(18,0) as franchise_id,
	group_code::varchar(500) as group_code,
	group_name::varchar(500) as group_name,
	group_id::number(18,0) as group_id,
	brand_group_1_code::varchar(500) as brand_group_1_code,
	brand_group_1_name::varchar(500) as brand_group_1_name,
	brand_group_1_id::number(18,0) as brand_group_1_id,
	brand_group_2_code::varchar(500) as brand_group_2_code,
	brand_group_2_name::varchar(500) as brand_group_2_name,
	brand_group_2_id::number(18,0) as brand_group_2_id,
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