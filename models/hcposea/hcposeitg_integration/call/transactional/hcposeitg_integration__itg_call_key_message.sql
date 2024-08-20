{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="
        DELETE FROM {{ this }} WHERE (Call_Key_Message_Id) IN (
        SELECT Call_Key_Message_Id FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_key_message') }} CALL_KEY_MESSAGE
        WHERE CALL_KEY_MESSAGE.Call_Key_Message_Id = Call_Key_Message_Id);
        "
    )
}}

with sdl_hcp_osea_call_key_message as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_call_key_message') }}
),

itg_key_message as (
    select * from dev_dna_core.hcposeitg_integration.itg_key_message
),

result as (

select sckm.call_key_message_id,
  sckm.call_key_message_name,
  sckm.call_key_message_createddate,
  sckm.call_key_message_createdbyid,
  sckm.last_modified_date,
  sckm.call_key_message_lastmodifiedbyid,
  sckm.call_key_message_systemmodstamp,
  sckm.call_key_message_account,
  sckm.call_key_message_call2,
  sckm.call_key_message_reaction,
  sckm.call_key_message_product,
  sckm.call_key_message_key_message,
  sckm.call_key_message_contact,
  sckm.call_key_message_call_date,
  sckm.call_key_message_user,
  sckm.call_key_message_category,
  sckm.call_key_message_vehicle,
  sckm.call_key_message_is_parent_call,
  sckm.call_key_message_clm_id,
  sckm.call_key_message_slide_version,
  sckm.call_key_message_duration,
  sckm.call_key_message_presentation_id,
  sckm.call_key_message_start_time,
  sckm.call_key_message_attendee_type,
  sckm.call_key_message_entity_reference_id,
  sckm.call_key_message_segment,
  sckm.call_key_message_display_order,
  sckm.call_key_message_detail_group,
  sckm.call_key_message_clm_presentation_name,
  ikm.country_code,
  (
    case 
      when upper(sckm.call_key_message_isdeleted) = 'TRUE'
        then 1
      when upper(sckm.call_key_message_isdeleted) = 'FALSE'
        then 0
      else 0
      end
    ) as call_key_message_isdeleted,
  sckm.key_message_description,
  sckm.call_key_message_clm_presentation_version,
  sckm.call_key_message_clm_presentation,
  sckm.key_message_name,
  current_timestamp(),
  current_timestamp()
from sdl_hcp_osea_call_key_message sckm
join itg_key_message ikm on (sckm.call_key_message_key_message = ikm.key_message_id)
),

final as (
    select 
        call_key_message_id::varchar(18) as call_key_message_id,
        call_key_message_name::varchar(300) as call_key_message_name,
        call_key_message_createddate::timestamp_ntz(9) as call_key_message_createddate,
        call_key_message_createdbyid::varchar(18) as call_key_message_createdbyid,
        last_modified_date::timestamp_ntz(9) as last_modified_date,
        call_key_message_lastmodifiedbyid::varchar(18) as call_key_message_lastmodifiedbyid,
        call_key_message_systemmodstamp::timestamp_ntz(9) as call_key_message_systemmodstamp,
        call_key_message_account::varchar(18) as call_key_message_account,
        call_key_message_call2::varchar(18) as call_key_message_call2,
        call_key_message_reaction::varchar(800) as call_key_message_reaction,
        call_key_message_product::varchar(18) as call_key_message_product,
        call_key_message_key_message::varchar(18) as call_key_message_key_message,
        call_key_message_contact::varchar(18) as call_key_message_contact,
        call_key_message_call_date::date as call_key_message_call_date,
        call_key_message_user::varchar(18) as call_key_message_user,
        call_key_message_category::varchar(800) as call_key_message_category,
        call_key_message_vehicle::varchar(800) as call_key_message_vehicle,
        call_key_message_is_parent_call::number(18,0) as call_key_message_is_parent_call,
        call_key_message_clm_id::varchar(500) as call_key_message_clm_id,
        call_key_message_slide_version::varchar(300) as call_key_message_slide_version,
        call_key_message_duration::number(18,2) as call_key_message_duration,
        call_key_message_presentation_id::varchar(300) as call_key_message_presentation_id,
        call_key_message_start_time::timestamp_ntz(9) as call_key_message_start_time,
        call_key_message_attendee_type::varchar(800) as call_key_message_attendee_type,
        call_key_message_entity_reference_id::varchar(300) as call_key_message_entity_reference_id,
        call_key_message_segment::varchar(300) as call_key_message_segment,
        call_key_message_display_order::number(18,0) as call_key_message_display_order,
        call_key_message_detail_group::varchar(18) as call_key_message_detail_group,
        call_key_message_clm_presentation_name::varchar(300) as call_key_message_clm_presentation_name,
        country_code::varchar(8) as country_code,
        call_key_message_isdeleted::number(38,0) as call_key_message_isdeleted,
        call_key_message_clm_presentation_version::varchar(100) as call_key_message_clm_presentation_version,
        call_key_message_clm_presentation::varchar(18) as call_key_message_clm_presentation,
        key_message_name::varchar(80) as key_message_name,
        key_message_description::varchar(1300) as key_message_description,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        current_timestamp()::timestamp_ntz(9) as updated_date
    from result
)

select * from final