with itg_hcp360_veeva_survey_question as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_survey_question') }}
),
final as
(
    select
    md5(survey_qn_id||country_code) as survey_question_key,
    md5(survey||country_code) as survey_key,
    nvl(country_code,'zz') as country_code,
    survey_qn_id as survey_question_source_id,
    name as survey_question_name,
    qn_order as survey_question_order,
    text as survey_question_text,
    answer_choice as survey_answer_choice,
    max_score,
    min_score,
    required,
    survey as survey_source_id,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    FROM itg_hcp360_veeva_survey_question

    UNION ALL

    SELECT 'NOT APPLICABLE','NOT APPLICABLE','ZZ','NOT APPLICABLE','NOT APPLICABLE','0','NOT APPLICABLE',
    'NOT APPLICABLE',0,0,'0','NOT APPLICABLE',convert_timezone('UTC',current_timestamp())::timestamp_ntz,convert_timezone('UTC',current_timestamp())::timestamp_ntz
)
select survey_question_key::varchar(32) as survey_question_key,
    survey_key::varchar(32) as survey_key,
    country_code::varchar(18) as country_code,
    survey_question_source_id::varchar(18) as survey_question_source_id,
    survey_question_name::varchar(80) as survey_question_name,
    survey_question_order::varchar(18) as survey_question_order,
    survey_question_text::varchar(1000) as survey_question_text,
    survey_answer_choice::varchar(700) as survey_answer_choice,
    max_score::varchar(100) as max_score,
    min_score::varchar(100) as min_score,
    required::varchar(5) as required,
    survey_source_id::varchar(18) as survey_source_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final