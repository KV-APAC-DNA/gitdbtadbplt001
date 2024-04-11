{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['time','cycle_year'],
        pre_hook= "delete from {{this}} where (substring(time, 0, 8), cycle_year) in (select substring(code, 0, 8) as cycle,cycle_year from {{ source('pcfsdl_raw', 'sdl_mds_pacific_perenso_cycle_dates') }});"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_perenso_cycle_dates') }}
),
final as 
(
select 
    code::varchar(50) as time,
	cycle_year::number(4,0) as cycle_year,
	start_date, --need to work here
	end_date, --need to work here
	null::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as update_dt 
from source
)
select * from final