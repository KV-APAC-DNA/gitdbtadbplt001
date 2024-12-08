{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (survey_question_id) in (select survey_question_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_survey_question') }} stg_survey_question
                where stg_survey_question.survey_question_id = survey_question_id);
                {% endif %}"
    )
}}
with sdl_hcp_osea_survey_question
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_survey_question') }}
)
,
transformed as
(
    select
    SURVEY_QUESTION_ID,
(CASE WHEN UPPER(IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(IS_DELETED) IS NULL THEN 0 WHEN UPPER(IS_DELETED) = ' ' 
 THEN 0 ELSE 0 END) AS IS_DELETED,
 SURVEY_QUESTION_NAME,
RECORD_TYPE_ID,
CREATED_DATE,
CREATED_BY_ID,
LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP,
MAY_EDIT,
CASE WHEN UPPER(IS_LOCKED) = 'TRUE' THEN 1 WHEN UPPER(IS_LOCKED) = 'FALSE' THEN 0 ELSE 0 END AS IS_LOCKED,	          
SURVEY_HEADER,
ANSWER_CHOICES,
EXTERNAL_ID,
MAX_SCORE,
MIN_SCORE,
QUESTION_ORDER,
QUESTION,
REQUIRED,
QUESTION_TEXT,
QUESTION_NUMBER,
CONDITION,
SOURCE_ID,
LEGACY_ID,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_survey_question
)
,
final as
(select 
	SURVEY_QUESTION_ID::VARCHAR(18) AS SURVEY_QUESTION_ID,
IS_DELETED::NUMBER(38,0) AS IS_DELETED,
SURVEY_QUESTION_NAME::VARCHAR(80) AS SURVEY_QUESTION_NAME,
RECORD_TYPE_ID::VARCHAR(18) AS RECORD_TYPE_ID,
CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP::TIMESTAMP_NTZ(9) AS SYSTEM_MODSTAMP,
MAY_EDIT::VARCHAR(10) AS MAY_EDIT,
IS_LOCKED::NUMBER(38,0) AS IS_LOCKED,
SURVEY_HEADER::VARCHAR(100) AS SURVEY_HEADER,
ANSWER_CHOICES::VARCHAR(700) AS ANSWER_CHOICES,
EXTERNAL_ID::VARCHAR(120) AS EXTERNAL_ID,
MAX_SCORE::NUMBER(11,0) AS MAX_SCORE,
MIN_SCORE::NUMBER(11,0) AS MIN_SCORE,
QUESTION_ORDER::NUMBER(3,0) AS QUESTION_ORDER,
QUESTION::VARCHAR(50) AS QUESTION,
REQUIRED::VARCHAR(50) AS REQUIRED,
QUESTION_TEXT::VARCHAR(1000) AS QUESTION_TEXT,
QUESTION_NUMBER::NUMBER(4,0) AS QUESTION_NUMBER,
CONDITION::VARCHAR(255) AS CONDITION,
SOURCE_ID::VARCHAR(255) AS SOURCE_ID,
LEGACY_ID::VARCHAR(255) AS LEGACY_ID,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
	from transformed
)

select * from final 
