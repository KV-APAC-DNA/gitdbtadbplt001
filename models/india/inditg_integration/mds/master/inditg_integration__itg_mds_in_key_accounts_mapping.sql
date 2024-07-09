with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_key_accounts_mapping') }}
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
	channel_name_code::varchar(500) as channel_name_code,
	channel_name_name::varchar(500) as channel_name_name,
	channel_name_id::number(18,0) as channel_name_id,
	account_name_code::varchar(500) as account_name_code,
	account_name_name::varchar(500) as account_name_name,
	account_name_id::number(18,0) as account_name_id,
	enterdatetime::timestamp_ntz(9) as enterdatetime,
	enterusername::varchar(200) as enterusername,
	enterversionnumber::number(18,0) as enterversionnumber,
	lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	lastchgusername::varchar(200) as lastchgusername,
	lastchgversionnumber::number(18,0) as lastchgversionnumber,
	validationstatus::varchar(500) as validationstatus
    from source
)
select * from final