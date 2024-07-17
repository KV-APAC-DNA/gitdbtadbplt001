with itg_hcp360_veeva_survey_target as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_survey_target') }}
),
itg_hcp360_veeva_question_response as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_question_response') }}
),
edw_hcp360_veeva_dim_hcp as
(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_hcp
),
edw_hcp360_veeva_dim_hco as
(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_hco
),
edw_hcp360_veeva_dim_employee as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_employee') }}
),
edw_hcp360_veeva_dim_survey as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_survey') }}
),
edw_hcp360_veeva_dim_survey_question as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_survey_question') }}
),
edw_hcp360_veeva_dim_country as
(
    select * from snapindedw_integration.edw_hcp360_veeva_dim_country
),
edw_vw_hcp360_date_dim as
(
    select * from {{ ref('hcpedw_integration__edw_vw_hcp360_date_dim') }}
),
final as
(
    select 
    nvl(dim_employee.employee_key,'NOT APPLICABLE') as employee_key,
    nvl(dim_hcp.hcp_key,'NOT APPLICABLE') as hcp_key,
    nvl(dim_hco.hco_key,'NOT APPLICABLE') as hco_key,
    nvl(dim_survey.survey_key,'NOT APPLICABLE') as survey_key,
    nvl(survey_question.survey_question_key,'NOT APPLICABLE') as survey_question_key,
    nvl(dim_country.country_key,'zz') as country_key,
    dim_date.cal_date_id as start_date_key,
    dim_date1.cal_date_id as end_date_key,
    dim_date2.cal_date_id as data_collection_date_key,
    survey_target.name as survey_response_name,
    question_response.question_text as survey_question_text,
    question_response.qn_order as survey_response_order,
    question_response.answer_choice as survey_answer_choice,
    question_response.response as survey_response_text,
    question_response.qn_number as survey_response_number,
    question_response.qn_date as survey_response_date,
    question_response.qn_datetime as survey_response_datetime,
    survey_target.score as survey_response_score,
    survey_target.status as survey_response_status,
    survey_target.channels as survey_response_channel,
    survey_target.survey_target_id as survey_target_source_id,
    question_response.qn_response_id as question_response_source_id,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
    null as survey_start_date
    from itg_hcp360_veeva_survey_target survey_target 
    inner join itg_hcp360_veeva_question_response question_response on survey_target.survey_target_id=question_response.survey_target 
    left join edw_hcp360_veeva_dim_employee dim_employee on survey_target.ownerid=dim_employee.employee_source_id 
    left join edw_hcp360_veeva_dim_hcp  dim_hcp on survey_target.account=dim_hcp.hcp_source_id 
    left join edw_hcp360_veeva_dim_hco dim_hco on survey_target.account=dim_hco.hco_source_id 
    left join edw_hcp360_veeva_dim_survey dim_survey on dim_survey.survey_source_id = survey_target.survey
    left join edw_hcp360_veeva_dim_survey_question survey_question on question_response.survey_question=survey_question.survey_question_source_id 
    left join edw_hcp360_veeva_dim_country dim_country on survey_target.country_code=dim_country.country_code 
    left join edw_vw_hcp360_date_dim dim_date on survey_target.start_date=dim_date.cal_date 
    left join edw_vw_hcp360_date_dim dim_date1 on survey_target.end_date=dim_date1.cal_date 
    left join edw_vw_hcp360_date_dim dim_date2 on question_response.qn_date=dim_date2.cal_date 
)
select employee_key::varchar(32) as employee_key,
    hcp_key::varchar(32) as hcp_key,
    hco_key::varchar(32) as hco_key,
    survey_key::varchar(32) as survey_key,
    survey_question_key::varchar(32) as survey_question_key,
    country_key::varchar(32) as country_key,
    start_date_key::varchar(32) as start_date_key,
    end_date_key::varchar(32) as end_date_key,
    data_collection_date_key::varchar(32) as data_collection_date_key,
    survey_response_name::varchar(500) as survey_response_name,
    survey_question_text::varchar(1000) as survey_question_text,
    survey_response_order::varchar(18) as survey_response_order,
    survey_answer_choice::varchar(6000) as survey_answer_choice,
    survey_response_text::varchar(6000) as survey_response_text,
    survey_response_number::varchar(18) as survey_response_number,
    survey_response_date::timestamp_ntz(9) as survey_response_date,
    survey_response_datetime::timestamp_ntz(9) as survey_response_datetime,
    survey_response_score::varchar(18) as survey_response_score,
    survey_response_status::varchar(18) as survey_response_status,
    survey_response_channel::varchar(255) as survey_response_channel,
    survey_target_source_id::varchar(18) as survey_target_source_id,
    question_response_source_id::varchar(18) as question_response_source_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm,
    survey_start_date::timestamp_ntz(9) as survey_start_date
 from final