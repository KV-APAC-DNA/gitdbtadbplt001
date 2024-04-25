{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['filename'],
        pre_hook= "delete from {{this}} where split_part(filename, '_', 1) in (
        select distinct split_part(filename, '_', 1) as filename from {{ source('phlsdl_raw', 'sdl_ph_clobotics_survey_data') }});"
    )
}}

with sdl_ph_clobotics_survey_data as 
(
    select * from {{ source('phlsdl_raw', 'sdl_ph_clobotics_survey_data') }}
),
itg_ph_clobotics_store_mapping as 
(
    select * from {{ source('phlitg_integration', 'itg_ph_clobotics_store_mapping') }} 
),
trans as
(
    select 
    project,
    period,
    taskid,
    plan_id,
    plan_status,
    plan_finish_time,
    username,
    user_display_name,
    store_code,
    store_name,
    city,
    channel,
    task_name,
    task_type,
    task_status,
    create_time,
    task_action_time,
    qn_code,
    qn_name,
    question_code,
    question_content,
    question_answer_code,
    question_answer_name,
    question_answer_value,
    question_answer_photos,
    filename,
    run_id
    from sdl_ph_clobotics_survey_data    
),
final as 
(
    select 
    a.project::varchar(255) as project,
	a.period::varchar(255) as period,
	a.taskid::varchar(255) as taskid,
	a.plan_id::varchar(255) as plan_id,
	a.plan_status::varchar(255) as plan_status,
    cast(a.plan_finish_time as timestamp_ntz(9)) as plan_finish_time,
    a.username::varchar(200) as username,
	a.user_display_name::varchar(200) as user_display_name,
    trim(b.store_code)::varchar(255) as store_code,
    a.store_name::varchar(255) as store_name,
	a.city::varchar(255) as city,
	a.channel::varchar(255) as channel,
	a.task_name::varchar(100) as task_name,
	a.task_type::varchar(100) as task_type,
	a.task_status::varchar(50) as task_status,
    cast(a.create_time as timestamp_ntz(9)) as create_time,
    cast(a.task_action_time as timestamp_ntz(9)) as task_action_time,
    a.qn_code::varchar(200) as qn_code,
	a.qn_name::varchar(255) as qn_name,
	a.question_code::varchar(255) as question_code,
	a.question_content::varchar(255) as question_content,
	a.question_answer_code::varchar(100) as question_answer_code,
	a.question_answer_name::varchar(255) as question_answer_name,
	a.question_answer_value::varchar(100) as question_answer_value,
	a.question_answer_photos::varchar(255) as question_answer_photos,
	a.filename::varchar(200) as filename,
	a.run_id::varchar(14) as run_id,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
from trans as a 
    left join itg_ph_clobotics_store_mapping b on 
    trim(a.store_code) = trim(b.old_store_code)
    and split_part(trim(a.store_name), ' ', 1) = split_part(trim(b.store_name), ' ', 1)
)  

select * from final