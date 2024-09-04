WITH itg_remote_meeting
AS (
  SELECT *
  FROM {{ ref('hcposeitg_integration__itg_remote_meeting') }}
  ),
trns
AS (
  SELECT md5(remote_meeting_source_id || country_code) AS remote_meeting_key,
    country_code,
    meeting_id,
    meeting_name,
    scheduled_flag,
    scheduled_datetime,
    latest_meeting_start_datetime AS latest_start_datetime,
    left(attendance_report_process_status, LENGTH(attendance_report_process_status) - 4) AS attendance_report_process_status,
    left(meeting_outcome_status, LENGTH(meeting_outcome_status) - 4) AS meeting_outcome_status,
    remote_meeting_source_id,
    owner_source_id,
    description,
    JJ_NumOfAttendee,
    current_timestamp() AS inserted_date
  FROM itg_remote_meeting
  WHERE is_deleted = 'false'
  ),
final
AS (
  SELECT remote_meeting_key::VARCHAR(200) AS remote_meeting_key,
    country_code::VARCHAR(10) AS country_code,
    meeting_id::VARCHAR(18) AS meeting_id,
    meeting_name::VARCHAR(255) AS meeting_name,
    scheduled_flag::VARCHAR(10) AS scheduled_flag,
    scheduled_datetime::timestamp_ntz(9) AS scheduled_datetime,
    latest_start_datetime::timestamp_ntz(9) AS latest_start_datetime,
    attendance_report_process_status::VARCHAR(200) AS attendance_report_process_status,
    meeting_outcome_status::VARCHAR(255) AS meeting_outcome_status,
    remote_meeting_source_id::VARCHAR(18) AS remote_meeting_source_id,
    owner_source_id::VARCHAR(18) AS owner_source_id,
    description::VARCHAR(1000) AS description,
    JJ_NumOfAttendee::number(18, 1) AS num_of_attendee,
    inserted_date::timestamp_ntz(9) AS inserted_date
  FROM trns
  )
SELECT *
FROM final
