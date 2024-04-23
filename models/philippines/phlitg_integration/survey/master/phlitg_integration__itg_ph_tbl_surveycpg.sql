with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveycpg') }}
),
final as
(
select
    custcode::varchar(10) as custcode,
	slsperid::varchar(10) as slsperid,
	branchcode::varchar(30) as branchcode,
	createddate::timestamp_ntz(9) as createddate,
	visitdate::timestamp_ntz(9) as visitdate,
	filename::varchar(50) as filename,
	filename_dt::number(14,0) as filename_dt,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm 
from source
)
select * from final