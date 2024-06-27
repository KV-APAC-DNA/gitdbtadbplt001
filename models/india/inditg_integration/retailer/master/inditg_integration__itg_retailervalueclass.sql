{{
    config
    (
        materializede="incremental",
        incremental_strategy="delete+insert",
        unique_key=["distcode","ctgmainid","classid"]
    )
}}
with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_retailervalueclass') }}
),
trans as 
(
    select * from 
    (
    select 
    sdl_rvc.classid::number(18,0) as classid,
	sdl_rvc.valueclasscode::varchar(40) as valueclasscode,
	sdl_rvc.valueclassname::varchar(100) as valueclassname,
	sdl_rvc.ctgmainid::number(18,0) as ctgmainid,
	sdl_rvc.isactive::boolean as isactive,
	sdl_rvc.distcode::varchar(100) as distcode,
	sdl_rvc.rowid::varchar(40) as rowid,
	sdl_rvc.filename::varchar(100) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    row_number() over (partition by sdl_rvc.classid,sdl_rvc.ctgmainid,upper(sdl_rvc.distcode) order by sdl_rvc.crt_dttm desc) rnum
      from source sdl_rvc)
where rnum = '1'
)
,
final as 
(
    select 
    classid,
    valueclasscode,
    valueclassname,
    ctgmainid,
    isactive,
    distcode,
    rowid,
    filename,
    crt_dttm,
    updt_dttm
from trans
)
select * from final