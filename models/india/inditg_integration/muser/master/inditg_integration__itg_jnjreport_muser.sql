{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["musername"],
        merge_exclude_columns= ["crt_dttm", "updt_dttm"],
        pre_hook  = "delete from {{this}} itg where itg.file_name  in (select sdl.file_name from
        {{ source('indsdl_raw', 'sdl_muser') }}) sdl) "
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_muser') }}
),
final as 
(
    select
	NULL::number(38,0) as musercode,
	src.UserCode::varchar(50) as musername,
	NULL::varchar(50) as muserpassword,
	UserFirstName::varchar(50) as mfirstname,
	UserLastName::varchar(50) as mlastname,
	EmailID::varchar(200) as memailid,
	createddate::timestamp_ntz(9) as userdateofcreation,
	NULL::timestamp_ntz(9) as userdateoflastpasschange,
	NULL::number(18,0) as userpassfailcount,
	NULL::varchar(1) as userdeleted,
	NULL::varchar(50) as encryptedusername,
	NULL::varchar(50) as logintime,
	NULL::varchar(50) as middlename,
	NULL::varchar(2000) as personaladdress,
	NULL::varchar(20) as phonenumber,
	NULL::varchar(20) as mobilenumber,
	NULL::varchar(20) as emergencynumber,
	isactive::varchar(1) as isactive,
	zonecode::varchar(50) as zonecode,
	regioncode::varchar(50) as regioncode,
	wwid::varchar(15) as wwsapid,
	NULL::varchar(1) as usertype,
	NULL::number(18,0) as groupcode,
	NULL::number(18,0) as designationcode,
	NULL::number(18,0) as dateofbirth,
	NULL::number(18,0) as monthofbirth,
	NULL::varchar(1) as ipppolicy,
	NULL::timestamp_ntz(9) as dateoflastlogin,
	territorycode::varchar(50) as territorycode,
	NULL::varchar(100) as distcode,
	NULL::number(18,0) as plantcode,
	NULL::timestamp_ntz(9) as lastloginfailedon,
	NULL::varchar(1) as issecretquestionset,
	NULL::number(18,0) as secretqfailcount,
	NULL::number(18,0) as nwcregion,
	NULL::number(18,0) as nwczone,
	NULL::number(18,0) as nwcterritory, 
	current_timestamp()::timestamp_ntz(9) AS crt_dttm,
	current_timestamp()::timestamp_ntz(9) AS updt_dttm,
    file_name:: varchar(255) as file_name
	FROM
	(Select distinct * from source) src
)
select * from final