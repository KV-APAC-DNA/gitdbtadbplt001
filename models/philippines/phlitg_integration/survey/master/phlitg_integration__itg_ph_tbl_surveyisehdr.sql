with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveyisehdr') }}
),
final as
(
select
    iseid::varchar(40) as iseid,
	isedesc::varchar(80) as isedesc,
	channelcode::varchar(10) as channelcode,
	channeldesc::varchar(50) as channeldesc,
	startdate::timestamp_ntz(9) as startdate,
	enddate::timestamp_ntz(9) as enddate,
	filename::varchar(50) as filename,
	filename_dt::number(14,0) as filename_dt,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm 
from source
)
select * from final