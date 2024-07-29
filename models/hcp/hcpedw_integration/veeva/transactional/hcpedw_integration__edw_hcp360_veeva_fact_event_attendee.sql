with itg_hcp360_veeva_event_attendee as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_event_attendee') }}
),
itg_hcp360_veeva_medical_event as
(
    select * from {{ source('hcpitg_integration', 'itg_hcp360_veeva_medical_event') }}
),
edw_hcp360_veeva_dim_hcp as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_hcp') }}
),
edw_hcp360_veeva_dim_product_indication as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_product_indication') }}
),
edw_hcp360_veeva_dim_employee as
(
    select * from {{ ref('hcpedw_integration__edw_hcp360_veeva_dim_employee') }}
),
final as
(
    select nvl(eve.medical_event_key,'Not Applicable') as medical_event_key,
    nvl(hcp.hcp_key,'Not Applicable') as Attendee_hcp_key,
    nvl(prod.product_indication_key,'Not Applicable') as product_indication_key,
    nvl(emp.employee_key,'Not Applicable') as attendee_employee_key,
    nvl(own_emp.employee_key,'Not Applicable') as owner_employee_key,
    nvl(att.country_code,'Not Applicable') as country_key,
    nvl(att.event_attendee_key, 'Not Applicable' ) as event_attendee_key,
    att.event_attendee_source_id,
    att.isdeleted as is_deleted,
    att.name  ,
    att.recordtypeid as record_type_id,
    att.createddate as created_date,
    att.createdbyid  as created_by_id,
    att.last_modified_date ,
    att.lastmodifiedbyid as last_modified_by_id,
    att.islocked as is_locked,
    att.attendee ,
    att.event_user as user_source_id,
    att.medical_event as medical_event_source_id,
    att.attendee_type ,
    att.status ,
    att.contact as contact_source_id,
    att.attendee_name ,
    att.account_source_id ,
    att.start_date   ,
    att.signature_datetime ,
    att.signature  ,
    att.jj_core_is_speaker as core_is_speaker,
    att.mobile_id,
    att.jj_in_attendance as in_attendance,
    att.invited_by as invited_by_id,
    NULL as position,
    att.jj_tw_attendee_details as tw_attendee_details,
    att.jj_event_owner as event_owner,
    att.jj_hcp_account_attachments_copied as hcp_account_attachments_copied,
    att.jj_tw_audience_name as tw_audience_name,
    att.jj_tw_audience_organization as tw_audience_organization,
    att.jj_tw_audience_position as tw_audience_position,
    att.jj_tw_audience_specialty as tw_audience_specialty,
    att.jj_tw_current_user_can_edit as tw_current_user_can_edit,
    att.jj_tw_duration_hrs as tw_duration_hrs,
    att.jj_tw_parent_account as tw_parent_account,
    att.jj_tw_position  as tw_position,
    att.jj_tw_service_end_date as tw_service_end_date,
    NULL as core_event_product,
    att.jj_tw_service_start_date as tw_service_start_date,
    att.jj_tw_specialty  as tw_specialty,
    att.jj_tw_primaryparent_sector as tw_primaryparent_sector,
    att.jj_tw_role as tw_role,
    att.jj_tw_comments as tw_comments,
    att.jj_service_fee as service_fee,
    att.jj_hcc_kol as hcc_kol,
    att.jj_about_role as about_role,
    NULL as vn_attendee_name,
    att.country_code,
    att.jj_tw_service_end_time as tw_service_end_time,
    att.jj_tw_service_start_time as tw_service_start_time,
    att.attendee_id18 ,
    att.parent_account_id18 ,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm
    from itg_hcp360_veeva_event_attendee att
    left join itg_hcp360_veeva_medical_event eve
    on att.medical_event = eve.medical_event_source_id and eve.country_code = att.country_code
    left join edw_hcp360_veeva_dim_hcp  hcp
    on hcp.hcp_source_id = att.account_source_id  and hcp.country_code = att.country_code
    left join (select distinct product_indication_key, product_name, product_source_id, country_code from edw_hcp360_veeva_dim_product_indication where country_code = 'IN') prod 
    on  prod.product_name=eve.primary_product and prod.country_code = eve.country_code
    left join edw_hcp360_veeva_dim_employee emp 
    on emp.employee_source_id = att.createdbyid and emp.country_code = att.country_code
    left join edw_hcp360_veeva_dim_employee own_emp 
    on own_emp.employee_source_id = eve.ownerid and own_emp.country_code = eve.country_code
    where  att.isdeleted = 0
)
select medical_event_key::varchar(32) as medical_event_key,
    attendee_hcp_key::varchar(32) as attendee_hcp_key,
    product_indication_key::varchar(32) as product_indication_key,
    attendee_employee_key::varchar(32) as attendee_employee_key,
    owner_employee_key::varchar(32) as owner_employee_key,
    country_key::varchar(32) as country_key,
    event_attendee_key::varchar(32) as event_attendee_key,
    event_attendee_source_id::varchar(18) as event_attendee_source_id,
    is_deleted::varchar(8) as is_deleted,
    name::varchar(256) as name,
    record_type_id::varchar(18) as record_type_id,
    created_date::timestamp_ntz(9) as created_date,
    created_by_id::varchar(18) as created_by_id,
    last_modified_date::timestamp_ntz(9) as last_modified_date,
    last_modified_by_id::varchar(18) as last_modified_by_id,
    is_locked::varchar(8) as is_locked,
    attendee::varchar(100) as attendee,
    user_source_id::varchar(18) as user_source_id,
    medical_event_source_id::varchar(100) as medical_event_source_id,
    attendee_type::varchar(1300) as attendee_type,
    status::varchar(256) as status,
    contact_source_id::varchar(18) as contact_source_id,
    attendee_name::varchar(1300) as attendee_name,
    account_source_id::varchar(18) as account_source_id,
    start_date::date as start_date,
    signature_datetime::timestamp_ntz(9) as signature_datetime,
    signature::varchar(32768) as signature,
    core_is_speaker::varchar(25) as core_is_speaker,
    mobile_id::varchar(100) as mobile_id,
    in_attendance::varchar(255) as in_attendance,
    invited_by_id::varchar(500) as invited_by_id,
    position::varchar(256) as position,
    tw_attendee_details::varchar(500) as tw_attendee_details,
    event_owner::varchar(1300) as event_owner,
    hcp_account_attachments_copied::varchar(25) as hcp_account_attachments_copied,
    tw_audience_name::varchar(50) as tw_audience_name,
    tw_audience_organization::varchar(100) as tw_audience_organization,
    tw_audience_position::varchar(50) as tw_audience_position,
    tw_audience_specialty::varchar(50) as tw_audience_specialty,
    tw_current_user_can_edit::varchar(25) as tw_current_user_can_edit,
    tw_duration_hrs::number(18,2) as tw_duration_hrs,
    tw_parent_account::varchar(1300) as tw_parent_account,
    tw_position::varchar(60) as tw_position,
    tw_service_end_date::date as tw_service_end_date,
    core_event_product::varchar(1300) as core_event_product,
    tw_service_start_date::date as tw_service_start_date,
    tw_specialty::varchar(1300) as tw_specialty,
    tw_primaryparent_sector::varchar(1300) as tw_primaryparent_sector,
    tw_role::varchar(256) as tw_role,
    tw_comments::varchar(500) as tw_comments,
    service_fee::number(18,2) as service_fee,
    hcc_kol::varchar(1300) as hcc_kol,
    about_role::varchar(1300) as about_role,
    vn_attendee_name::varchar(1300) as vn_attendee_name,
    country_code::varchar(2) as country_code,
    tw_service_end_time::varchar(256) as tw_service_end_time,
    tw_service_start_time::varchar(256) as tw_service_start_time,
    attendee_id18::varchar(1300) as attendee_id18,
    parent_account_id18::varchar(1300) as parent_account_id18,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final