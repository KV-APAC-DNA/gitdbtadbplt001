{{
    config
    (
        materializede="incremental",
        incremental_strategy="delete+insert",
        unique_key=["distributorcode","routecode"]
    )
}}
with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_rrl_routemaster') }}
),
trans as 
(
    select * from 
    (
    select 
    sdl_rtm.routecode::varchar(50) as routecode,
	sdl_rtm.routeename::varchar(100) as routeename,
	sdl_rtm.flag::varchar(2) as flag,
	sdl_rtm.isactive::boolean as isactive,
	sdl_rtm.distributorcode::varchar(28) as distributorcode,
	sdl_rtm.rowid::varchar(40) as rowid,
	sdl_rtm.filename::varchar(100) as filename,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
    row_number() over (partition by upper(sdl_rtm.routecode),upper(sdl_rtm.distributorcode) order by sdl_rtm.crt_dttm desc) rnum
      from source sdl_rtm)
where rnum = '1'
),
final as 
(
    select 
    routecode,
    routeename,
    flag,
    isactive,
    distributorcode,
    rowid,
    filename,
    crt_dttm,
    updt_dttm
from trans
)
select * from final