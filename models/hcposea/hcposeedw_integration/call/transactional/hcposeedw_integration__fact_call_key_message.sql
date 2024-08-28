
{{
    config(
        materialized= 'incremental',
        incremental_strategy= 'append',
        pre_hook = ["{% if is_incremental() %}
                     delete from {{this}} where(call_key_message_source_id) in (select call_key_message_id from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL_KEY_MESSAGE);
                    {% endif %}"]
    )
}}
with itg_call_key_message as (
select * from {{ ref('hcposeitg_integration__itg_call_key_message') }}
),
itg_call as (
select * from {{ ref('hcposeitg_integration__itg_call') }}
),
itg_call_discussion as (
select * from {{ ref('hcposeitg_integration__itg_call_discussion') }}
),
itg_call_detail as (
select * from {{ ref('hcposeitg_integration__itg_call_detail') }}
),
dim_product_indication as (
select * from {{ ref('hcposeedw_integration__dim_product_indication') }}
),
dim_hcp as (
select * from {{ ref('hcposeedw_integration__dim_hcp') }}
),
dim_hco as (
select * from {{ ref('hcposeedw_integration__dim_hco') }}
),
dim_date as (
select * from {{ ref('hcposeedw_integration__dim_date') }}
),
dim_employee as (
select * from {{ ref('hcposeedw_integration__dim_employee') }}
),
dim_profile as (
select * from {{ ref('hcposeedw_integration__dim_profile') }}
),
dim_organization as (
select * from {{ ref('hcposeedw_integration__dim_organization') }}
),
dim_country as (
select * from {{ ref('hcposeedw_integration__dim_country') }}
),
itg_lookup_retention_period as (
select * from {{ source('hcposeitg_integration', 'itg_lookup_retention_period') }}
),
transformed as (
select DISTINCT
NVL(DC.country_KEY,'ZZ') as country_key,
NVL(HCP.HCP_KEY,'Not Applicable') as hcp_key,
NVL(HCP.PARENT_HCO_KEY,(nvl(HCO.hco_key,'Not Applicable'))) as hco_key,
nvl(us_pr.employee_key,'Not Applicable') as employee_key,
nvl(us_pr.organization_key,'Not Applicable') as organization_key,
nvl(us_pr.profile_key,'Not Applicable') as profile_key,
nvl(DPI.product_indication_key, 'Not Applicable' ) as product_indication_key,
NVL(DD.Date_Key,cast(to_char(to_date(ICK.Call_Key_Message_Call_Date),'yyyymmdd') as integer)) as call_date_key,
MD5(ICK.Call_Key_Message_Key_Message||NVL(DC.country_KEY,'ZZ')) as key_message_key,
ICK.Call_Key_Message_Account as hcp_source_id,
ICK.Call_Key_Message_Attendee_Type as call_attendance_type,
ICK.Call_Key_Message_Call2 as call_source_id,
ICK.Call_Key_Message_Call_Date as call_date,
ICK.Call_Key_Message_Category as category,
ICK.Call_Key_Message_CLM_ID as clm_id,
ICK.Call_Key_Message_Contact as contact_source_id,
ICK.Call_Key_Message_CreatedById as call_key_message_created_by,
ICK.Call_Key_Message_CreatedDate as call_key_message_created_date,
ICK.Call_Key_Message_Detail_Group as call_key_message_detail_group,
ICK.Call_Key_Message_Display_Order as call_key_message_clm_display_order,
ICK.Call_Key_Message_Duration as call_key_message_duration,
ICK.Call_Key_Message_Entity_Reference_Id as call_key_message_entity_reference_id,
ICK.Call_Key_Message_Id as call_key_message_source_id,
ICK.Call_Key_Message_Is_Parent_Call as call_key_message_parent_call_flag,
ICK.Call_Key_Message_Clm_Presentation_Name as call_key_message_clm_presentation_name,
ICK.Call_Key_Message_Key_Message as key_message_source_id,
ICK.Call_Key_Message_LastModifiedById as call_key_message_last_modified_by,
ICK.Last_Modified_Date as call_key_message_last_modified_date,
ICK.Call_Key_Message_Name as call_key_message_name,
ICK.Call_Key_Message_Presentation_ID as call_key_message_presentation_source_id,
ICK.Call_Key_Message_Product as product_source_id,
ICK.Call_Key_Message_Reaction as call_key_message_reaction,
ICK.Call_Key_Message_Segment as call_key_message_segment,
ICK.Call_Key_Message_Slide_Version as call_key_message_slide_version,
ICK.Call_Key_Message_Start_Time as call_key_message_start_time,
ICK.Call_Key_Message_SystemModstamp as call_key_message_modify_timestamp,
ICK.Call_Key_Message_User as call_key_message_source_user,
ICK.Call_Key_Message_Vehicle as call_key_message_vehicle,
CDT.call_detail_source_id as call_detail_source_id,
CDS.call_discussion_source_id as call_discussion_source_id,
SYSDATE() as call_key_message_data_inserted_date,
SYSDATE() as call_key_message_data_updated_date
FROM itg_call_key_message ICK
LEFT JOIN itg_call IC
ON (ICK.call_key_message_call2 = IC.CALL_SOURCE_ID AND IC.is_deleted=0)
LEFT JOIN itg_call_discussion CDS
ON (IC.CALL_SOURCE_ID = CDS.CALL_SOURCE_ID  AND ICK.Call_Key_Message_Product = CDS.PRODUCT_SOURCE_ID AND CDS.is_deleted=0) 
LEFT JOIN itg_call_detail CDT
ON (IC.CALL_SOURCE_ID = CDT.CALL_SOURCE_ID AND ICK.Call_Key_Message_Product = CDT.PRODUCT_SOURCE_ID AND CDT.is_deleted=0)
left join dim_product_indication DPI
ON(ICK.Call_Key_Message_Product=DPI.product_source_id)  
LEFT JOIN dim_hcp HCP
ON(ICK.Call_Key_Message_Account = HCP.hcp_source_id)
LEFT JOIN dim_hco HCO
ON(ICK.Call_Key_Message_Account = HCO.hco_source_id)
LEFT JOIN dim_date DD
ON(NVL(ICK.Call_Key_Message_Call_Date,to_date(SYSDATE()))=to_date(DD.DATE_VALUE))
LEFT JOIN ( select employee_key,employee_profile_id ,employee_source_id,pf.profile_source_id, us.profile_name , profile_key, us.country_code ,o.organization_key,o.effective_from_date ,o.effective_to_date
            from dim_employee us, 
                 dim_profile pf,
                dim_organization o
            where us.employee_profile_id = pf.profile_source_id 
            and   us.country_code = pf.country_code 
            and   us.my_organization_code = o.my_organization_code(+)
            and   us.country_code = o.country_code (+)) us_pr
ON (IC.owner_source_id = us_pr.employee_source_id
AND NVL(ICK.Call_Key_Message_Call_Date,to_date(SYSDATE())) >= nvl(us_pr.effective_from_date,to_date('1900-01-01 00:00:00','yyyy-mm-dd hh24:mi:ss'))
AND NVL(ICK.Call_Key_Message_Call_Date,to_date(SYSDATE())) <= nvl(us_pr.effective_to_date,to_date('2099-12-31 00:00:00','yyyy-mm-dd hh24:mi:ss'))
)
left join dim_country DC
ON(ICK.COUNTRY_CODE=DC.country_code)
WHERE NVL(ICK.Call_Key_Message_Call_Date,to_date(SYSDATE())) > (select DATE_TRUNC('year', dateadd(day,-retention_years*365,SYSDATE())) 
                                                            from itg_lookup_retention_period
                                                            where UPPER(table_name) = 'FACT_CALL_KEY_MESSAGE')
),
final as (
select
country_key::varchar(15) as country_key,
hcp_key::varchar(32) as hcp_key,
hco_key::varchar(80) as hco_key,
employee_key::varchar(32) as employee_key,
organization_key::varchar(32) as organization_key,
profile_key::varchar(32) as profile_key,
product_indication_key::varchar(32) as product_indication_key,
call_date_key::number(38,0) as call_date_key,
key_message_key::varchar(256) as key_message_key,
hcp_source_id::varchar(18) as hcp_source_id,
call_attendance_type::varchar(255) as call_attendance_type,
call_source_id::varchar(18) as call_source_id,
call_date::date as call_date,
category::varchar(255) as category,
clm_id::varchar(100) as clm_id,
contact_source_id::varchar(18) as contact_source_id,
call_key_message_created_by::varchar(18) as call_key_message_created_by,
call_key_message_created_date::timestamp_ntz(9) as call_key_message_created_date,
call_key_message_detail_group::varchar(18) as call_key_message_detail_group,
call_key_message_clm_display_order::number(18,0) as call_key_message_clm_display_order,
call_key_message_duration::number(18,2) as call_key_message_duration,
call_key_message_entity_reference_id::varchar(100) as call_key_message_entity_reference_id,
call_key_message_source_id::varchar(18) as call_key_message_source_id,
call_key_message_parent_call_flag::number(18,0) as call_key_message_parent_call_flag,
call_key_message_clm_presentation_name::varchar(400) as call_key_message_clm_presentation_name,
key_message_source_id::varchar(18) as key_message_source_id,
call_key_message_last_modified_by::varchar(18) as call_key_message_last_modified_by,
call_key_message_last_modified_date::timestamp_ntz(9) as call_key_message_last_modified_date,
call_key_message_name::varchar(80) as call_key_message_name,
call_key_message_presentation_source_id::varchar(100) as call_key_message_presentation_source_id,
product_source_id::varchar(18) as product_source_id,
call_key_message_reaction::varchar(255) as call_key_message_reaction,
call_key_message_segment::varchar(80) as call_key_message_segment,
call_key_message_slide_version::varchar(100) as call_key_message_slide_version,
call_key_message_start_time::timestamp_ntz(9) as call_key_message_start_time,
call_key_message_modify_timestamp::timestamp_ntz(9) as call_key_message_modify_timestamp,
call_key_message_source_user::varchar(18) as call_key_message_source_user,
call_key_message_vehicle::varchar(255) as call_key_message_vehicle,
call_detail_source_id::varchar(18) as call_detail_source_id,
call_discussion_source_id::varchar(18) as call_discussion_source_id,
call_key_message_data_inserted_date::timestamp_ntz(9) as call_key_message_data_inserted_date,
call_key_message_data_updated_date::timestamp_ntz(9) as call_key_message_data_updated_date
from transformed
)
select * from final
