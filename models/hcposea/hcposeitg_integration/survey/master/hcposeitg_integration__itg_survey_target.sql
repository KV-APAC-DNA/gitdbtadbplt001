{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (survey_target_id) in (select survey_target_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_survey_target') }} stg_survey_target
                where stg_survey_target.survey_target_id = survey_target_id);
                {% endif %}"
    )
}}
with sdl_hcp_osea_survey_target
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_survey_target') }}
)
,
transformed as
(
    select
    SURVEY_TARGET_ID,
OWNER_ID,
(CASE WHEN UPPER(IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(IS_DELETED) IS NULL THEN 0 WHEN UPPER(IS_DELETED) = ' ' 
 THEN 0 ELSE 0 END) AS IS_DELETED,
SURVEY_TARGET,
RECORD_TYPE_ID,
CREATED_DATE,
CREATED_BY_ID,
LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP,
MAY_EDIT,
CASE WHEN UPPER(IS_LOCKED) = 'TRUE' THEN 1 WHEN UPPER(IS_LOCKED) = 'FALSE' THEN 0 ELSE 0 END AS IS_LOCKED,	          
LAST_VIEWED_DATE,
LAST_REFERENCED_DATE,
ACCOUNT_DISPLAY_NAME,
ACCOUNT,
CHANNELS,
END_DATE,
ENTITY_REFERENCE_ID,
EXTERNAL_ID,
LANGUAGE,
LOCK,
MOBILE_ID,
DONT_CHANGE_OWNER_ON_PUBLISH,
NOT_COMPLETED,
REGION,
SEGMENT,
START_DATE,
STATUS,
SURVEY_NAME,
TERRITORY,
ADDRESS,
SPECIALTY,
SCORE,
ACCOUNT_TYPES,
INCLUDED_USER_TERRITORIES,
USER,
MANAGER,
COMPLETED_DATETIME,
DETAIL_GROUP,
EMPLOYEE,
PRODUCT,
PROFILE_NAME,
RECALLED_DATETIME,
REPORT_STATUS,
REVIEW_DATE,
SENT_TO_REP_DATETIME,
SHARE_TEAM,
ZVOD_COACHING_QUESTIONS_VOD,
PRIMARY_PARENT,
SENT_EMAIL,
ATTENDEE,
EVENT_SPEAKER,
EVENT,
ZVOD_QUESTIONS_VOD,
SUGGESTION,
SUBMITTED_DATETIME,
ACCOUNT_SPECIALTY,
SURVEY_RESULTS_SHARING_SCOPE,
EMPLOYEE_COMMENT,
ENABLE_EMPLOYEE_COMMENT,
REP_MANAGER,
REP_DEPARTMENT,
REP_DIVISION,
ACCOUNTID18,
CHILD_ACCOUNT,
LOCATION_ENTITY_REFERENCE_ID,
LOCATION,
TARGET_TYPE,
ACCOUNT_DATA_PRIVACY_REQUESTED,
SURVEY_TYPE,
SURVEY_MINDSET_PRODUCT_BRAND_NAME,
SURVEY_PARENT_PRODUCT_ID,
SURVEY_PRODUCT_ID,
SURVEY_PRODUCT_NAME,
COUNTRY_CODE,
SEGMENT_METRIC_UPDATED_FLAG,
LEVEL_2_MANAGER,
CONTENT,
CALL_ID,
CALL,
EMPLOYEE_EMAIL,
HYPERLINK_FOR_RECORD,
JJ_LEGACY_ID,
PRIMARY_PARENT_ID18,
ALLOW_EMAIL_ALERTS_FROM_SURVEY,
ALLOWED_BACK_DAYS_FROM_SURVEY,
ALLOWED_BACK_DAYS_TO_ACKNOWLEDGE,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_survey_target
)
,
final as
(select 
	SURVEY_TARGET_ID::VARCHAR(18) AS SURVEY_TARGET_ID,
OWNER_ID::VARCHAR(80) AS OWNER_ID,
IS_DELETED::NUMBER(38,0) AS IS_DELETED,
SURVEY_TARGET::VARCHAR(80) AS SURVEY_TARGET,
RECORD_TYPE_ID::VARCHAR(18) AS RECORD_TYPE_ID,
CREATED_DATE::TIMESTAMP_NTZ(9) AS CREATED_DATE,
CREATED_BY_ID::VARCHAR(18) AS CREATED_BY_ID,
LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
LAST_MODIFIED_BY_ID::VARCHAR(18) AS LAST_MODIFIED_BY_ID,
SYSTEM_MODSTAMP::TIMESTAMP_NTZ(9) AS SYSTEM_MODSTAMP,
MAY_EDIT::VARCHAR(10) AS MAY_EDIT,
IS_LOCKED::NUMBER(38,0) AS IS_LOCKED,
LAST_VIEWED_DATE::TIMESTAMP_NTZ(9) AS LAST_VIEWED_DATE,
LAST_REFERENCED_DATE::TIMESTAMP_NTZ(9) AS LAST_REFERENCED_DATE,
ACCOUNT_DISPLAY_NAME::VARCHAR(100) AS ACCOUNT_DISPLAY_NAME,
ACCOUNT::VARCHAR(80) AS ACCOUNT,
CHANNELS::VARCHAR(255) AS CHANNELS,
END_DATE::DATE AS END_DATE,
ENTITY_REFERENCE_ID::VARCHAR(100) AS ENTITY_REFERENCE_ID,
EXTERNAL_ID::VARCHAR(120) AS EXTERNAL_ID,
LANGUAGE::VARCHAR(80) AS LANGUAGE,
LOCK::VARCHAR(80) AS LOCK,
MOBILE_ID::VARCHAR(100) AS MOBILE_ID,
DONT_CHANGE_OWNER_ON_PUBLISH::VARCHAR(50) AS DONT_CHANGE_OWNER_ON_PUBLISH,
NOT_COMPLETED::VARCHAR(1300) AS NOT_COMPLETED,
REGION::VARCHAR(255) AS REGION,
SEGMENT::VARCHAR(255) AS SEGMENT,
START_DATE::DATE AS START_DATE,
STATUS::VARCHAR(100) AS STATUS,
SURVEY_NAME::VARCHAR(100) AS SURVEY_NAME,
TERRITORY::VARCHAR(255) AS TERRITORY,
ADDRESS::VARCHAR(100) AS ADDRESS,
SPECIALTY::VARCHAR(50) AS SPECIALTY,
SCORE::VARCHAR(11) AS SCORE,
ACCOUNT_TYPES::VARCHAR(5000) AS ACCOUNT_TYPES,
INCLUDED_USER_TERRITORIES::VARCHAR(5000) AS INCLUDED_USER_TERRITORIES,
USER::VARCHAR(50) AS USER,
MANAGER::VARCHAR(50) AS MANAGER,
COMPLETED_DATETIME::TIMESTAMP_NTZ(9) AS COMPLETED_DATETIME,
DETAIL_GROUP::VARCHAR(50) AS DETAIL_GROUP,
EMPLOYEE::VARCHAR(50) AS EMPLOYEE,
PRODUCT::VARCHAR(50) AS PRODUCT,
PROFILE_NAME::VARCHAR(255) AS PROFILE_NAME,
RECALLED_DATETIME::TIMESTAMP_NTZ(9) AS RECALLED_DATETIME,
REPORT_STATUS::VARCHAR(100) AS REPORT_STATUS,
REVIEW_DATE::DATE AS REVIEW_DATE,
SENT_TO_REP_DATETIME::TIMESTAMP_NTZ(9) AS SENT_TO_REP_DATETIME,
SHARE_TEAM::VARCHAR(80) AS SHARE_TEAM,
ZVOD_COACHING_QUESTIONS_VOD::VARCHAR(80) AS ZVOD_COACHING_QUESTIONS_VOD,
PRIMARY_PARENT::VARCHAR(1300) AS PRIMARY_PARENT,
SENT_EMAIL::VARCHAR(100) AS SENT_EMAIL,
ATTENDEE::VARCHAR(100) AS ATTENDEE,
EVENT_SPEAKER::VARCHAR(100) AS EVENT_SPEAKER,
EVENT::VARCHAR(100) AS EVENT,
ZVOD_QUESTIONS_VOD::VARCHAR(100) AS ZVOD_QUESTIONS_VOD,
SUGGESTION::VARCHAR(100) AS SUGGESTION,
SUBMITTED_DATETIME::TIMESTAMP_NTZ(9) AS SUBMITTED_DATETIME,
ACCOUNT_SPECIALTY::VARCHAR(1300) AS ACCOUNT_SPECIALTY,
SURVEY_RESULTS_SHARING_SCOPE::VARCHAR(40) AS SURVEY_RESULTS_SHARING_SCOPE,
EMPLOYEE_COMMENT::VARCHAR(32000) AS EMPLOYEE_COMMENT,
ENABLE_EMPLOYEE_COMMENT::VARCHAR(50) AS ENABLE_EMPLOYEE_COMMENT,
REP_MANAGER::VARCHAR(1300) AS REP_MANAGER,
REP_DEPARTMENT::VARCHAR(1300) AS REP_DEPARTMENT,
REP_DIVISION::VARCHAR(1300) AS REP_DIVISION,
ACCOUNTID18::VARCHAR(1300) AS ACCOUNTID18,
CHILD_ACCOUNT::VARCHAR(100) AS CHILD_ACCOUNT,
LOCATION_ENTITY_REFERENCE_ID::VARCHAR(100) AS LOCATION_ENTITY_REFERENCE_ID,
LOCATION::VARCHAR(50) AS LOCATION,
TARGET_TYPE::VARCHAR(100) AS TARGET_TYPE,
ACCOUNT_DATA_PRIVACY_REQUESTED::VARCHAR(50) AS ACCOUNT_DATA_PRIVACY_REQUESTED,
SURVEY_TYPE::VARCHAR(1300) AS SURVEY_TYPE,
SURVEY_MINDSET_PRODUCT_BRAND_NAME::VARCHAR(1300) AS SURVEY_MINDSET_PRODUCT_BRAND_NAME,
SURVEY_PARENT_PRODUCT_ID::VARCHAR(1300) AS SURVEY_PARENT_PRODUCT_ID,
SURVEY_PRODUCT_ID::VARCHAR(1300) AS SURVEY_PRODUCT_ID,
SURVEY_PRODUCT_NAME::VARCHAR(1300) AS SURVEY_PRODUCT_NAME,
COUNTRY_CODE::VARCHAR(1300) AS COUNTRY_CODE,
SEGMENT_METRIC_UPDATED_FLAG::VARCHAR(100) AS SEGMENT_METRIC_UPDATED_FLAG,
LEVEL_2_MANAGER::VARCHAR(1300) AS LEVEL_2_MANAGER,
CONTENT::VARCHAR(100) AS CONTENT,
CALL_ID::VARCHAR(20) AS CALL_ID,
CALL::VARCHAR(100) AS CALL,
EMPLOYEE_EMAIL::VARCHAR(1300) AS EMPLOYEE_EMAIL,
HYPERLINK_FOR_RECORD::VARCHAR(1300) AS HYPERLINK_FOR_RECORD,
JJ_LEGACY_ID::VARCHAR(255) AS JJ_LEGACY_ID,
PRIMARY_PARENT_ID18::VARCHAR(1300) AS PRIMARY_PARENT_ID18,
ALLOW_EMAIL_ALERTS_FROM_SURVEY::VARCHAR(100) AS ALLOW_EMAIL_ALERTS_FROM_SURVEY,
ALLOWED_BACK_DAYS_FROM_SURVEY::NUMBER(18,0) AS ALLOWED_BACK_DAYS_FROM_SURVEY,
ALLOWED_BACK_DAYS_TO_ACKNOWLEDGE::NUMBER(18,0) AS ALLOWED_BACK_DAYS_TO_ACKNOWLEDGE,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
	from transformed
)

select * from final 