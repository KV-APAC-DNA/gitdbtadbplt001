{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook =
            "{% if is_incremental() %}
                delete from {{this}} itg where itg.filename  in (select sdl.filename from
                {{ source('phlsdl_raw','sdl_ph_tbl_surveychoices') }} sdl where filename not in (
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveychoices__null_test')}}
                union all
                select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveychoices__duplicate_test')}}
            ));
            {%endif%}"
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveychoices') }}
    where filename not in (
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveychoices__null_test')}}
        union all
        select distinct file_name from {{source('phlwks_integration','TRATBL_sdl_ph_tbl_surveychoices__duplicate_test')}}
    )
),
final as
(
select
    iseid::varchar(100) as iseid,
	quesno::number(18,0) as quesno,
	answerseq::number(18,0) as answerseq,
	skugroup::varchar(100) as skugroup,
	repparam::varchar(100) as repparam,
	putupid::varchar(20) as putupid,
	putupdesc::varchar(200) as putupdesc,
	score::number(18,0) as score,
	sfa::number(18,0) as sfa,
	brandid::varchar(30) as brandid,
	brand::varchar(120) as brand,
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