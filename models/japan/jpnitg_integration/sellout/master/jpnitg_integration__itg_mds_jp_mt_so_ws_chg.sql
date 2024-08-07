with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_mt_so_ws_chg') }}   
),

result as (
    select
        bgn_sndr_cd::varchar(200) as bgn_sndr_cd,
	    changetrackingmask::number(18,0) as changetrackingmask,
	    code::varchar(500) as code,
	    enterdatetime::timestamp_ntz(9) as enterdatetime,
	    enterusername::varchar(200) as enterusername,
	    enterversionnumber::number(18,0) as enterversionnumber,
	    error_type::number(28,0) as error_type,
	    id::number(18,0) as id,
	    int_partner_number::varchar(200) as int_partner_number,
	    lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	    lastchgusername::varchar(200) as lastchgusername,
	    lastchgversionnumber::number(18,0) as lastchgversionnumber,
	    muid::varchar(36) as muid,
	    name::varchar(500) as name,
	    validationstatus::varchar(500) as validationstatus,
	    version_id::number(18,0) as version_id,
	    versionflag::varchar(100) as versionflag,
	    versionname::varchar(100) as versionname,
	    versionnumber::number(18,0) as versionnumber,
	    ws_cd::varchar(200) as ws_cd,
        current_timestamp()::timestamp_ntz(9) as create_date
    from source
)

select * from result