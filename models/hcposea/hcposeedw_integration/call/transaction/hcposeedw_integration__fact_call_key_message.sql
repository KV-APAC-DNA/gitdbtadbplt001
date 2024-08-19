
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = ["{% if is_incremental() %}
                     delete from fact_call_key_message where(call_key_message_source_id) in (select call_key_message_id from itg_call_key_message);
                    {% endif %}"]
    )
}}
with itg_call_key_message as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL_KEY_MESSAGE
),
itg_call as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL
),
itg_call_discussion as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL_DISCUSSION
),
itg_call_detail as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_CALL_DETAIL
),
dim_product_indication as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_PRODUCT_INDICATION
),
dim_hcp as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_HCP
),
dim_hco as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_HCO
),
dim_date as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_DATE
),
dim_employee as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_EMPLOYEE
),
dim_profile as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_PROFILE
),
dim_organization as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_ORGANIZATION
),
dim_country as (
select * from DEV_DNA_CORE.HCPOSEEDW_INTEGRATION.DIM_COUNTRY
),
itg_lookup_retention_period as (
select * from DEV_DNA_CORE.HCPOSEITG_INTEGRATION.ITG_LOOKUP_RETENTION_PERIOD
),
final as (
select DISTINCT
NVL(DC.country_KEY,'ZZ') as COUNTRY_KEY,
NVL(HCP.HCP_KEY,'Not Applicable') as HCP_KEY,
NVL(HCP.PARENT_HCO_KEY,(nvl(HCO.hco_key,'Not Applicable'))) AS HCO_KEY,
nvl(us_pr.employee_key,'Not Applicable') AS EMPLOYEE_KEY,
nvl(us_pr.organization_key,'Not Applicable') AS ORGANIZATION_KEY,
nvl(us_pr.profile_key,'Not Applicable') as profile_key,
nvl(DPI.product_indication_key, 'Not Applicable' ) as PRODUCT_INDICATION_KEY,
NVL(DD.Date_Key,cast(to_char(to_date(ICK.Call_Key_Message_Call_Date),'yyyymmdd') as integer)),
MD5(ICK.Call_Key_Message_Key_Message||NVL(DC.country_KEY,'ZZ')),
ICK.Call_Key_Message_Account,
ICK.Call_Key_Message_Attendee_Type,
ICK.Call_Key_Message_Call2,
ICK.Call_Key_Message_Call_Date,
ICK.Call_Key_Message_Category,
ICK.Call_Key_Message_CLM_ID,
ICK.Call_Key_Message_Contact,
ICK.Call_Key_Message_CreatedById,
ICK.Call_Key_Message_CreatedDate,
ICK.Call_Key_Message_Detail_Group,
ICK.Call_Key_Message_Display_Order,
ICK.Call_Key_Message_Duration,
ICK.Call_Key_Message_Entity_Reference_Id,
ICK.Call_Key_Message_Id,
ICK.Call_Key_Message_Is_Parent_Call,
ICK.Call_Key_Message_Clm_Presentation_Name,
ICK.Call_Key_Message_Key_Message,
ICK.Call_Key_Message_LastModifiedById,
ICK.Last_Modified_Date,
ICK.Call_Key_Message_Name,
ICK.Call_Key_Message_Presentation_ID,
ICK.Call_Key_Message_Product,
ICK.Call_Key_Message_Reaction,
ICK.Call_Key_Message_Segment,
ICK.Call_Key_Message_Slide_Version,
ICK.Call_Key_Message_Start_Time,
ICK.Call_Key_Message_SystemModstamp,
ICK.Call_Key_Message_User,
ICK.Call_Key_Message_Vehicle,
CDT.call_detail_source_id,
CDS.call_discussion_source_id,
SYSDATE(),
SYSDATE()
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
)
select * from final
