with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_businessplan_brand') }}
),
final as 
(
    select 
    brand_dsr_code::varchar(500) as brand_dsr_code,
	brand_dsr_id::number(18,0) as brand_dsr_id,
	brand_dsr_name::varchar(500) as brand_dsr_name,
	business_plan::number(31,5) as business_plan,
	changetrackingmask::number(18,0) as changetrackingmask,
	cluster_code::varchar(500) as cluster_code,
	cluster_id::number(18,0) as cluster_id,
	cluster_name::varchar(500) as cluster_name,
	code::varchar(500) as code,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
	enterusername::varchar(200) as enterusername,
	enterversionnumber::number(18,0) as enterversionnumber,
	franchise_dsr_code::varchar(500) as franchise_dsr_code,
	franchise_dsr_id::number(18,0) as franchise_dsr_id,
	franchise_dsr_name::varchar(500) as franchise_dsr_name,
	gsm_code::varchar(500) as gsm_code,
	gsm_id::number(18,0) as gsm_id,
	gsm_name::varchar(500) as gsm_name,
	id::number(18,0) as id,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	month_code::varchar(500) as month_code,
	month_id::number(18,0) as month_id,
	month_name::varchar(500) as month_name,
	muid::varchar(36) as muid,
	name::varchar(500) as name,
	region_code::varchar(500) as region_code,
	region_id::number(18,0) as region_id,
	region_name::varchar(500) as region_name,
	validationstatus::varchar(500) as validationstatus,
	variant_name_code::varchar(500) as variant_name_code,
	variant_name_id::number(18,0) as variant_name_id,
	variant_name_name::varchar(500) as variant_name_name,
	variant_tableau_code::varchar(500) as variant_tableau_code,
	variant_tableau_id::number(18,0) as variant_tableau_id,
	variant_tableau_name::varchar(500) as variant_tableau_name,
	version_id::number(18,0) as version_id,
	versionflag::varchar(100) as versionflag,
	versionname::varchar(100) as versionname,
	versionnumber::number(18,0) as versionnumber,
	year_code::varchar(500) as year_code,
	year_id::number(18,0) as year_id,
	year_name::varchar(500) as year_name
    from source
)
select  * from final