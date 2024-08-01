with source as(
    select * from {{ source('jpnsdl_raw', 'sdl_mds_jp_c360_eng_translation') }}
),

result as(
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
	    name_jp::varchar(200) as name_jp,
	    name_eng::varchar(200) as name_eng,
	    enterdatetime::timestamp_ntz(9) as enterdatetime,
	    enterusername::varchar(200) as enterusername,
	    enterversionnumber::number(18,0) as enterversionnumber,
	    lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
	    lastchgusername::varchar(200) as lastchgusername,
	    lastchgversionnumber::number(18,0) as lastchgversionnumber,
	    validationstatus::varchar(500) as validationstatus,
        current_timestamp::timestamp_ntz(9) as create_date
    from source
)

select * from result