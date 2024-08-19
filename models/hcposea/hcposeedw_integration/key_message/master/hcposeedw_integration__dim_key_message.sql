
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                     delete from dim_key_message where (key_message_source_id) in (select key_message_id from itg_key_message);
                    {% endif %}"]
    )
}}
with itg_key_message as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_KEY_MESSAGE
),
itg_call_key_message as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL_KEY_MESSAGE
),
itg_call as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL
),
dim_country as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_COUNTRY
),
dim_product_indication as (
select * from DEV_DNA_CORE.SNAPOSEEDW_INTEGRATION.DIM_PRODUCT_INDICATION
),
final as (
select distinct 
md5(IKM.Key_Message_Id||nvl(IKM.Country_Code,'ZZ')) as Key_Message_Key,
nvl(IKM.Country_Code,'ZZ') as Country_Code,
IKM.Key_Message_Name,
NVL(PI.Product_Name,'Not Applicable') as PRODUCT_NAME,
IKM.Key_Message_Id,
IKM.Key_Message_CreatedDate,
IKM.Key_Message_CreatedById,
IKM.Last_Modified_Date,
IKM.Key_Message_LastModifiedById,
IKM.Key_Message_SystemModstamp,
IKM.Key_Message_LastViewedDate,
IKM.Key_Message_LastReferencedDate,
IKM.Key_Message_Description,
IKM.Key_Message_Active,
IKM.Key_Message_Category,
IKM.Key_Message_Vehicle,
IKM.Key_Message_CLM_ID,
IKM.Key_Message_Custom_Reaction,
IKM.Key_Message_Slide_Version,
IKM.Key_Message_Language,
IKM.Key_Message_Media_File_CRC,
IKM.Key_Message_Media_File_Name,
IKM.Key_Message_Media_File_Size,
IKM.Key_Message_Segment,
IKM.Key_Message_Detail_Group,
IKM.Key_Message_Core_Content_Approval_ID,
IKM.Key_Message_Core_Content_Expiration_Date,
IKM.Country_Code,
IKM.AP_CLM_Country,
IKM.Key_Message_Is_Shared_Resource,
IKM.Key_Message_Shared_Resource,
Sysdate(),
Sysdate()
FROM itg_key_message IKM
LEFT JOIN itg_call_key_message ICKM
ON(IKM.Key_Message_Id=ICKM.Call_Key_Message_Key_Message)
LEFT JOIN itg_call IC
ON(ICKM.Call_Key_Message_Call2=IC.CALL_SOURCE_ID)
LEFT JOIN dim_country DC
ON(IKM.Country_Code=DC.country_code)
LEFT JOIN dim_product_indication PI
ON(IKM.Key_Message_Product = PI.PRODUCT_SOURCE_ID)
)
select * from final

