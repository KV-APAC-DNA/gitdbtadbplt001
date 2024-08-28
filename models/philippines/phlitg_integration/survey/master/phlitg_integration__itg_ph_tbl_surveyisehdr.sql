{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook =
            "{% if is_incremental() %}
                delete from {{this}} itg where itg.filename  in (select sdl.filename from
                {{ source('phlsdl_raw','sdl_ph_tbl_surveyisehdr') }} sdl where filename not in (
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveyisehdr__null_test')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveyisehdr__duplicate_test')}}
            ));
            {%endif%}"
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveyisehdr') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveyisehdr__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveyisehdr__duplicate_test')}}
    )
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
	{% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
     where source.crt_dttm > (select max(crt_dttm) from {{ this }})
    {% endif %}
)
select * from final