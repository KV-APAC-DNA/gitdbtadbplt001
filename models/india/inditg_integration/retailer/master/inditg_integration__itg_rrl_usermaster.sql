{{
    config
    (
        materializede="incremental",
        incremental_strategy="delete+insert",
        unique_key=["userid"]
    )
}}
with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_usermaster') }}
),
trans as 
(
    select * from 
    (
    select 
    sdl_usm.userid::number(38,0) as userid,
	sdl_usm.usercode::varchar(50) as usercode,
	sdl_usm.login::varchar(200) as login,
	sdl_usm.password::varchar(100) as password,
	sdl_usm.eusername::varchar(100) as eusername,
	sdl_usm.userlevel::number(18,0) as userlevel,
	sdl_usm.parentid::varchar(100) as parentid,
	sdl_usm.isactive::boolean as isactive,
	sdl_usm.teritoryid::number(18,0) as teritoryid,
	sdl_usm.abnumber::varchar(100) as abnumber,
	sdl_usm.forumcode::varchar(100) as forumcode,
	sdl_usm.regionid::number(18,0) as regionid,
	sdl_usm.emailid::varchar(200) as emailid,
	sdl_usm.currentversion::varchar(20) as currentversion,
	sdl_usm.updateversion::varchar(20) as updateversion,
	sdl_usm.imei::varchar(100) as imei,
	sdl_usm.mobileno::varchar(50) as mobileno,
	sdl_usm.locationid::number(18,0) as locationid,
	sdl_usm.ishht::varchar(1) as ishht,
	sdl_usm.user_createddate::timestamp_ntz(9) as user_createddate,
	sdl_usm.distuserid::number(18,0) as distuserid,
	sdl_usm.freezeday::number(18,0) as freezeday,
	sdl_usm.filename::varchar(100) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    row_number() over (partition by sdl_usm.userid order by sdl_usm.crt_dttm desc) rnum
      from source sdl_usm)
where rnum = '1'
),
final as 
(
    select 
    userid,
    usercode,
    login,
    password,
    eusername,
    userlevel,
    parentid,
    isactive,
    teritoryid,
    abnumber,
    forumcode,
    regionid,
    emailid,
    currentversion,
    updateversion,
    imei,
    mobileno,
    locationid,
    ishht,
    user_createddate,
    distuserid,
    freezeday,
    filename,
    crt_dttm,
    updt_dttm
from trans
)
select * from final