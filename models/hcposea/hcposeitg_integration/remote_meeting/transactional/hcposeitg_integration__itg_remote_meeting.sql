{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete
                from {{this}}
                where remote_meeting_source_id in (select distinct remote_meeting_source_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_remote_meeting') }} a
                where a.remote_meeting_source_id = remote_meeting_source_id)
                {% endif %}"
                )
}}
with 
sdl_hcp_osea_remote_meeting
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_remote_meeting') }}
),
transformed 
as
(
SELECT remote_meeting_source_id,
       jj_owner_country as country_code,
       owner_source_id,
       is_deleted,
       name,
       record_type_source_id,
       created_date,
       createdbyid,
       last_modified_date,
       lastmodifiedbyid,
       systemmodstamp,
       mayedit,
       islocked,
       meeting_id,
       meeting_name,
       mobile_source_id,
       scheduled_datetime,
       scheduled as scheduled_flag,
       meeting_password,
       meeting_outcome_status,
       jj_host_country,
       jj_numofattendee,
       assigned_host,
       attendance_report_process_status,
       description,
       latest_meeting_start_datetime,
       webinar_alternative_host_1,
       webinar_alternative_host_2,
       webinar_alternative_host_3,
       current_timestamp() AS inserted_date,
       current_timestamp() AS updated_date,
       (CASE WHEN UPPER(Rating_Submitted) = 'TRUE' THEN 1 WHEN UPPER(Rating_Submitted) IS NULL THEN 0 WHEN UPPER(Rating_Submitted) = ' ' THEN 0 ELSE 0 END) AS Rating_Submitted
FROM SDL_HCP_OSEA_REMOTE_MEETING)
,
final as
(
    select 
	REMOTE_MEETING_SOURCE_ID::VARCHAR(18) as REMOTE_MEETING_SOURCE_ID,
	COUNTRY_CODE::VARCHAR(10) as COUNTRY_CODE,
	OWNER_SOURCE_ID::VARCHAR(18) as OWNER_SOURCE_ID,
	IS_DELETED::VARCHAR(10) as IS_DELETED,
	NAME::VARCHAR(255) as NAME,
	RECORD_TYPE_SOURCE_ID::VARCHAR(18) as RECORD_TYPE_SOURCE_ID,
	CREATED_DATE::TIMESTAMP_NTZ(9) as CREATED_DATE,
	CREATEDBYID::VARCHAR(18) as CREATEDBYID,
	LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) as LAST_MODIFIED_DATE,
	LASTMODIFIEDBYID::VARCHAR(18) as LASTMODIFIEDBYID,
	SYSTEMMODSTAMP::TIMESTAMP_NTZ(9) as SYSTEMMODSTAMP,
	MAYEDIT::VARCHAR(10) as MAYEDIT,
	ISLOCKED::VARCHAR(10) as ISLOCKED,
	MEETING_ID::VARCHAR(18) as MEETING_ID,
	MEETING_NAME::VARCHAR(255) as MEETING_NAME,
	MOBILE_SOURCE_ID::VARCHAR(100) as MOBILE_SOURCE_ID,
	SCHEDULED_DATETIME::TIMESTAMP_NTZ(9) as SCHEDULED_DATETIME,
	SCHEDULED_FLAG::VARCHAR(10) as SCHEDULED_FLAG,
	MEETING_PASSWORD::VARCHAR(20) as MEETING_PASSWORD,
	MEETING_OUTCOME_STATUS::VARCHAR(255) as MEETING_OUTCOME_STATUS,
	JJ_HOST_COUNTRY::VARCHAR(20) as JJ_HOST_COUNTRY,
	JJ_NUMOFATTENDEE::NUMBER(18,1) as JJ_NUMOFATTENDEE,
	ASSIGNED_HOST::VARCHAR(200) as ASSIGNED_HOST,
	ATTENDANCE_REPORT_PROCESS_STATUS::VARCHAR(200) as ATTENDANCE_REPORT_PROCESS_STATUS,
	DESCRIPTION::VARCHAR(1000) as DESCRIPTION,
	LATEST_MEETING_START_DATETIME::TIMESTAMP_NTZ(9) as LATEST_MEETING_START_DATETIME,
	WEBINAR_ALTERNATIVE_HOST_1::VARCHAR(200) as WEBINAR_ALTERNATIVE_HOST_1,
	WEBINAR_ALTERNATIVE_HOST_2::VARCHAR(200) as WEBINAR_ALTERNATIVE_HOST_2,
	WEBINAR_ALTERNATIVE_HOST_3::VARCHAR(200) as WEBINAR_ALTERNATIVE_HOST_3,
	INSERTED_DATE::TIMESTAMP_NTZ(9) as INSERTED_DATE,
	UPDATED_DATE::TIMESTAMP_NTZ(9) as UPDATED_DATE,
	RATING_SUBMITTED::VARCHAR(5) as RATING_SUBMITTED
    from transformed 
)

select * from final 

