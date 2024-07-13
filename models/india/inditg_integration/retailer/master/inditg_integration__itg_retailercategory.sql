{{
    config
    (
        materializede="incremental",
        incremental_strategy="delete+insert",
        unique_key=["retailercategorycode"]
    )
}}
with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_retailercategory') }}
),
trans as 
(
    select * from 
    (
    select 
    sdl_rtc.retailercategorycode::varchar(10) as retailercategorycode,
	sdl_rtc.retailercategoryname::varchar(50) as retailercategoryname,
	sdl_rtc.ctgcode::varchar(40) as ctgcode,
	sdl_rtc.ctglinkid::number(18,0) as ctglinkid,
	sdl_rtc.ctglevelid::number(18,0) as ctglevelid,
	sdl_rtc.isbrandshow::number(18,0) as isbrandshow,
	sdl_rtc.isactive::boolean as isactive,
	sdl_rtc.rowid::varchar(40) as rowid,
	sdl_rtc.filename::varchar(100) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    row_number() over (partition by upper(sdl_rtc.retailercategorycode) order by sdl_rtc.crt_dttm desc) rnum
    from source sdl_rtc)
where rnum = '1'
),
final as 
(
    select 
    retailercategorycode,
    retailercategoryname,
    ctgcode,
    ctglinkid,
    ctglevelid,
    isbrandshow,
    isactive,
    rowid,
    filename,
    crt_dttm,
    updt_dttm 
from trans
)
select * from final