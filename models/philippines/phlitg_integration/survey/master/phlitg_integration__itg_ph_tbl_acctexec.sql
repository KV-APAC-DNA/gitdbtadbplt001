with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_acctexec') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_acctexec__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_acctexec__duplicate_test')}}
    )
),
final as
(
select
    custcode::varchar(10) as custcode,
	slsperid::varchar(10) as slsperid,
	supcustcode::varchar(10) as supcustcode,
	supid::varchar(10) as supid,
	name::varchar(100) as name,
	positioncode::varchar(10) as positioncode,
	positiondesc::varchar(10) as positiondesc,
	filename::varchar(50) as filename,
	null::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm 
from source
)
select * from final