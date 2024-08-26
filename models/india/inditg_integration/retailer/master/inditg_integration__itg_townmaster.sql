{{
    config
    (
        materialized="incremental",
        incremental_strategy="delete+insert",
        unique_key=["rsrcode","rsdcode","routecode","villagecode"]
    )
}}
with source as 
(
    select *,dense_rank() over(partition by rsrcode,rsdcode,routecode,villagecode order by file_name desc) as rnk from {{ source('indsdl_raw', 'sdl_rrl_townmaster') }}
   
    where file_name not in (
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_rrl_townmaster__null_test')}}
        

) ) qualify rnk = 1,
trans as 
(
    select * from 
    (
    select 
    sdl_rtm.routecode::varchar(50) as routecode,
	sdl_rtm.villagecode::varchar(50) as villagecode,
	sdl_rtm.villagename::varchar(200) as villagename,
	sdl_rtm.population::number(38,0) as population,
	sdl_rtm.rsrcode::varchar(50) as rsrcode,
	sdl_rtm.rsdcode::varchar(50) as rsdcode,
	sdl_rtm.distributorcode::varchar(50) as distributorcode,
	sdl_rtm.sarpanchname::varchar(50) as sarpanchname,
	sdl_rtm.sarpanchno::varchar(50) as sarpanchno,
	sdl_rtm.isactive::boolean as isactive,
	sdl_rtm.createddate::timestamp_ntz(9) as createddate,
	sdl_rtm.createdby::varchar(50) as createdby,
	sdl_rtm.updateddate::timestamp_ntz(9) as updateddate,
	sdl_rtm.updatedby::varchar(50) as updatedby,
	sdl_rtm.filename::varchar(100) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    row_number() over (partition by upper(sdl_rtm.villagecode),upper(sdl_rtm.routecode),upper(sdl_rtm.rsdcode),upper(sdl_rtm.rsrcode) order by sdl_rtm.crt_dttm desc) rnum
      from source sdl_rtm)
where rnum = '1'
),
final as 
(
    select 
    routecode,
    villagecode,
    villagename,
    population,
    rsrcode,
    rsdcode,
    distributorcode,
    sarpanchname,
    sarpanchno,
    isactive,
    createddate,
    createdby,
    updateddate,
    updatedby,
    filename,
    crt_dttm,
    updt_dttm
from trans
)
select * from final