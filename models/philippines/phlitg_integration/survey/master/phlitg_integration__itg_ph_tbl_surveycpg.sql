{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook =
            "{% if is_incremental() %}
                delete from {{this}} itg where itg.filename  in (select sdl.filename from
                {{ source('phlsdl_raw','sdl_ph_tbl_surveycpg') }} sdl where filename not in (
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveycpg__null_test')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveycpg__duplicate_test')}}
            ));
            {%endif%}"
        
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveycpg') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveycpg__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveycpg__duplicate_test')}}
    )
),
final as
(
select
    custcode::varchar(10) as custcode,
	slsperid::varchar(10) as slsperid,
	branchcode::varchar(60) as branchcode,
	createddate::timestamp_ntz(9) as createddate,
	visitdate::timestamp_ntz(9) as visitdate,
	filename::varchar(50) as filename,
	filename_dt::number(14,0) as filename_dt,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
    {% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
     where source.crt_dttm > (select max(crt_dttm) from {{ this }})
    {% endif %}
)
select * from final