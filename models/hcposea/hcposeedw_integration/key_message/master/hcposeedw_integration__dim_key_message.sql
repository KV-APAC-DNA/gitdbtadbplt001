
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                     delete from {{ this }} where (key_message_source_id) in (select key_message_id from {{ ref('hcposeitg_integration__itg_key_message') }});
                    {% endif %}"]
    )
}}
with itg_key_message as (
select * from {{ ref('hcposeitg_integration__itg_key_message') }}
),
itg_call_key_message as (
select * from {{ ref('hcposeitg_integration__itg_call_key_message') }}
),
itg_call as (
select * from {{ ref('hcposeitg_integration__itg_call') }}
),
dim_country as (
select * from {{ ref('hcposeedw_integration__dim_country') }}
),
dim_product_indication as (
select * from {{ ref('hcposeedw_integration__dim_product_indication') }}
),
transformed as (
select distinct 
md5(IKM.Key_Message_Id||nvl(IKM.Country_Code,'ZZ')) as key_message_key,
nvl(IKM.Country_Code,'ZZ') as country_code,
IKM.Key_Message_Name as key_message_name,
NVL(PI.Product_Name,'Not Applicable') as product_name,
IKM.Key_Message_Id as key_message_source_id,
IKM.Key_Message_CreatedDate as key_message_createddate,
IKM.Key_Message_CreatedById as key_message_createdbyid,
IKM.Last_Modified_Date as key_message_lastmodifieddate,
IKM.Key_Message_LastModifiedById as key_message_lastmodifiedbyid,
IKM.Key_Message_SystemModstamp as key_message_systemmodstamp,
IKM.Key_Message_LastViewedDate as key_message_lastvieweddate,
IKM.Key_Message_LastReferencedDate as key_message_lastreferenceddate,
IKM.Key_Message_Description as key_message_description,
IKM.Key_Message_Active as key_message_active_flag,
IKM.Key_Message_Category as key_message_category,
IKM.Key_Message_Vehicle as key_message_vehicle,
IKM.Key_Message_CLM_ID as key_message_clm_id,
IKM.Key_Message_Custom_Reaction as key_message_custom_reaction,
IKM.Key_Message_Slide_Version as key_message_slide_version,
IKM.Key_Message_Language as key_message_language,
IKM.Key_Message_Media_File_CRC as key_message_media_file_crc,
IKM.Key_Message_Media_File_Name as key_message_media_file_name,
IKM.Key_Message_Media_File_Size as key_message_media_file_size,
IKM.Key_Message_Segment as key_message_segment,
IKM.Key_Message_Detail_Group as key_message_detail_group,
IKM.Key_Message_Core_Content_Approval_ID as key_message_core_content_approval_id,
IKM.Key_Message_Core_Content_Expiration_Date as key_message_core_content_expiration_date,
IKM.Country_Code as key_message_simp_country,
IKM.AP_CLM_Country as key_message_ap_clm_country,
IKM.Key_Message_Is_Shared_Resource as key_message_shared_resource_flag,
IKM.Key_Message_Shared_Resource as key_message_shared_resource,
current_timestamp() as inserted_date,
current_timestamp() as updated_date
FROM itg_key_message IKM
LEFT JOIN itg_call_key_message ICKM
ON(IKM.Key_Message_Id=ICKM.Call_Key_Message_Key_Message)
LEFT JOIN itg_call IC
ON(ICKM.Call_Key_Message_Call2=IC.CALL_SOURCE_ID)
LEFT JOIN dim_country DC
ON(IKM.Country_Code=DC.country_code)
LEFT JOIN dim_product_indication PI
ON(IKM.Key_Message_Product = PI.PRODUCT_SOURCE_ID)
),
final as (
select
key_message_key::varchar(256) as key_message_key,
country_code::varchar(256) as country_code,
key_message_name::varchar(250) as key_message_name,
product_name::varchar(80) as product_name,
key_message_source_id::varchar(18) as key_message_source_id,
key_message_createddate::timestamp_ntz(9) as key_message_createddate,
key_message_createdbyid::varchar(18) as key_message_createdbyid,
key_message_lastmodifieddate::timestamp_ntz(9) as key_message_lastmodifieddate,
key_message_lastmodifiedbyid::varchar(18) as key_message_lastmodifiedbyid,
key_message_systemmodstamp::timestamp_ntz(9) as key_message_systemmodstamp,
key_message_lastvieweddate::timestamp_ntz(9) as key_message_lastvieweddate,
key_message_lastreferenceddate::timestamp_ntz(9) as key_message_lastreferenceddate,
key_message_description::varchar(800) as key_message_description,
key_message_active_flag::varchar(5) as key_message_active_flag,
key_message_category::varchar(255) as key_message_category,
key_message_vehicle::varchar(255) as key_message_vehicle,
key_message_clm_id::varchar(100) as key_message_clm_id,
key_message_custom_reaction::varchar(255) as key_message_custom_reaction,
key_message_slide_version::varchar(100) as key_message_slide_version,
key_message_language::varchar(255) as key_message_language,
key_message_media_file_crc::varchar(255) as key_message_media_file_crc,
key_message_media_file_name::varchar(255) as key_message_media_file_name,
key_message_media_file_size::number(18,0) as key_message_media_file_size,
key_message_segment::varchar(80) as key_message_segment,
key_message_detail_group::varchar(18) as key_message_detail_group,
key_message_core_content_approval_id::varchar(100) as key_message_core_content_approval_id,
key_message_core_content_expiration_date::date as key_message_core_content_expiration_date,
key_message_simp_country::varchar(1300) as key_message_simp_country,
key_message_ap_clm_country::varchar(4) as key_message_ap_clm_country,
key_message_shared_resource_flag::number(38,0) as key_message_shared_resource_flag,
key_message_shared_resource::varchar(18) as key_message_shared_resource,
inserted_date::timestamp_ntz(9) as inserted_date,
updated_date::timestamp_ntz(9) as updated_date
from transformed
)
select * from final

