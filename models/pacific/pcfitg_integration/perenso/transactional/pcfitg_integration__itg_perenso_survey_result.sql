{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['store_chk_hdr_key','line_key','todo_key','prod_grp_key'],
        pre_hook= "delete from {{this}} where (nvl(store_chk_hdr_key,'999999'),nvl(line_key,'999999'),nvl(todo_key,'999999'),nvl(prod_grp_key,'999999')) in (select distinct nvl(store_chk_hdr_key,'999999'),
       nvl(line_key,'999999'),nvl(todo_key,'999999'),nvl(prod_grp_key,'999999') from {{ source('pcfsdl_raw', 'sdl_perenso_survey_result') }});
       delete from {{this}} where store_chk_hdr_key in (select distinct store_chk_hdr_key from 
       {% if target.name=='prod' %} pcfedw_integration.edw_perenso_survey {% else %} {{schema}}.pcfedw_integration__edw_perenso_survey {% endif %} 
       where store_chk_date>=dateadd(day,-91,convert_timezone('UTC',current_timestamp)::date));"
    )
}}
with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_perenso_survey_result') }}
),
final as 
(
    select 
    store_chk_hdr_key::number(10,0) as store_chk_hdr_key,
	line_key::number(10,0) as line_key,
	todo_key::number(10,0) as todo_key,
	prod_grp_key::number(10,0) as prod_grp_key,
	optionans::number(10,0) as optionans,
	notesans::varchar(16777216) as notesans,
	run_id::number(14,0) as run_id,
	current_timestamp()::timestamp_ntz(9) as create_dt,
	current_timestamp()::timestamp_ntz(9) as update_dt 
    from source
)
select * from final