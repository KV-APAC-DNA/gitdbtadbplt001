with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_isebranchmaster') }}
    where filename not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_tbl_isebranchmaster__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_tbl_isebranchmaster__duplicate_test')}}
    )
),
final as
(
select
    branchcode::varchar(200) as branchcode,
	branchname::varchar(100) as branchname,
	area::varchar(50) as area,
	channel::varchar(50) as channel,
	region::varchar(50) as region,
	state::varchar(50) as state,
	city::varchar(50) as city,
	district::varchar(50) as district,
	parentcode::varchar(20) as parentcode,
	parentname::varchar(50) as parentname,
	tradetype::varchar(20) as tradetype,
	storeprioritization::varchar(100) as storeprioritization,
	channelcode::varchar(10) as channelcode,
	latitude::number(10,4) as latitude,
	longitude::number(10,4) as longitude,
	filename::varchar(50) as filename,
	null::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm 
from source
)
select * from final