{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_tbl_surveyisequestion') }}
    where filename not in (
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_tbl_surveyisequestion__null_test')}}
        union all
        select distinct file_name from {{SOURCE('phlwks_integration','TRATBL_sdl_ph_tbl_surveyisequestion__duplicate_test')}}
    )
),
final as
(
select
    iseid::varchar(100) as iseid,
	quesno::number(18,0) as quesno,
	quescode::varchar(10) as quescode,
	quesdesc::varchar(200) as quesdesc,
	standardques_flag::number(18,0) as standardques_flag,
	quesclasscode::varchar(100) as quesclasscode,
	quesclassdesc::varchar(100) as quesclassdesc,
	weigh::number(10,4) as weigh,
	totalscore::number(10,4) as totalscore,
	answercode::varchar(10) as answercode,
	answerdesc::varchar(200) as answerdesc,
	franchisecode::varchar(30) as franchisecode,
	franchisedesc::varchar(100) as franchisedesc,
	product_flag::number(18,0) as product_flag,
	picture_flag::number(18,0) as picture_flag,
	nopicture_flag::number(18,0) as nopicture_flag,
	note_flag::number(18,0) as note_flag,
	filename::varchar(50) as filename,
	filename_dt::number(14,0) as filename_dt,
	null::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as crt_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm
from source
	{% if is_incremental() %}
    -- -- this filter will only be applied on an incremental run
     where source.crt_dttm > (select max(crt_dttm) from {{ this }})
    {% endif %}
)
select * from final