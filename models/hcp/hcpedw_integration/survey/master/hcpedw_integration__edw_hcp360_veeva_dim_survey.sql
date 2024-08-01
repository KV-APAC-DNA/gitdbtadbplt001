with itg_hcp360_veeva_survey as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_survey') }}
),
edw_vw_hcp360_date_dim as
(
    select * from {{ ref('hcpedw_integration__edw_vw_hcp360_date_dim') }}
),
final as
(
    select
    nvl(md5(survey.survey_id||survey.country_code),'not applicable') as survey_key,
    NULL as product_indication_key,
    date2.cal_date_id as survey_start_date_key,
    date1.cal_date_id as survey_end_date_key,
    nvl(survey.country_code,'zz') as country_code,
    survey.survey_id as survey_source_id,
    survey.name as survey_name,
    survey.assignment_type as survey_assignment_type,
    survey.status as survey_status,
    survey.channels as survey_channel,
    NULL as survey_category,
    survey.territory as survey_territory,
    survey.max_score as survey_max_score,
    survey.min_score as survey_min_score,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from  itg_hcp360_veeva_survey survey
    left join edw_vw_hcp360_date_dim date2 on survey.start_date=date2.cal_date
    left join edw_vw_hcp360_date_dim date1 on survey.end_date=date1.cal_date 

    UNION ALL

    SELECT 'NOT APPLICABLE','NOT APPLICABLE',NULL,NULL,'ZZ','NOT APPLICABLE','NOT APPLICABLE',
    'NOT APPLICABLE','NOT APPLICABLE','NOT APPLICABLE','NOT APPLICABLE','NOT APPLICABLE',0,0,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz,convert_timezone('UTC',current_timestamp())::timestamp_ntz 
)
select survey_key::varchar(32) as survey_key,
    product_indication_key::varchar(32) as product_indication_key,
    survey_start_date_key::varchar(32) as survey_start_date_key,
    survey_end_date_key::varchar(32) as survey_end_date_key,
    country_code::varchar(8) as country_code,
    survey_source_id::varchar(18) as survey_source_id,
    survey_name::varchar(80) as survey_name,
    survey_assignment_type::varchar(255) as survey_assignment_type,
    survey_status::varchar(255) as survey_status,
    survey_channel::varchar(255) as survey_channel,
    survey_category::varchar(255) as survey_category,
    survey_territory::varchar(255) as survey_territory,
    survey_max_score::varchar(18) as survey_max_score,
    survey_min_score::varchar(18) as survey_min_score,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final