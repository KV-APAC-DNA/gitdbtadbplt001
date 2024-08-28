{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (key_message_id) in (select key_message_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_key_message') }} stg_key_message
                where stg_key_message.key_message_id = key_message_id);
                {% endif %}"
    )
}}

with sdl_hcp_osea_key_message
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_key_message') }}
)
,
transformed as
(
select key_message_id,
       key_message_ownerid,
       key_message_name,
       key_message_createddate,
       key_message_createdbyid,
       last_modified_date,
       key_message_lastmodifiedbyid,
       key_message_systemmodstamp,
       key_message_lastvieweddate,
       key_message_lastreferenceddate,
       key_message_description,
       key_message_product,
       key_message_product_strategy,
       (case when upper(key_message_active) = 'true' then 1 when upper(key_message_active) is null then 0 when upper(key_message_active) = ' ' then 0 else 0 end) as key_message_active,
       key_message_category,
       key_message_vehicle,
       key_message_clm_id,
       key_message_custom_reaction,
       key_message_slide_version,
       key_message_language,
       key_message_media_file_crc,
       key_message_media_file_name,
       key_message_media_file_size,
       key_message_segment,
       key_message_detail_group,
       key_message_core_content_approval_id,
       key_message_core_content_expiration_date,
       simp_country as country_code,
       ap_clm_country,
       (case when upper(key_message_is_shared_resource) = 'true' then 1 when upper(key_message_is_shared_resource) is null then 0 when upper(key_message_is_shared_resource) = ' ' then 0 else 0 end) as key_message_is_shared_resource,
       key_message_shared_resource,
       key_message_cdn_path,
       key_message_display_order,
       ap_country,
       functional_team,
       janssen_code,
       key_message_recordtypeid,
       key_message_status,
       key_message_vexternal_id,
       sysdate() as inserted_date,
       sysdate() as updated_date,
       vault_document_id,
key_message_group  ,
   key_message_sub_group ,
   key_message_purpose ,
   key_message_content_topic ,
   key_message_content_sub_topic 
from sdl_hcp_osea_key_message
)
,
final as
(select 
	key_message_id::varchar(18) as key_message_id,
	key_message_ownerid::varchar(18) as key_message_ownerid,
	key_message_name::varchar(250) as key_message_name,
	key_message_createddate::timestamp_ntz(9) as key_message_createddate,
	key_message_createdbyid::varchar(18) as key_message_createdbyid,
	last_modified_date::timestamp_ntz(9) as last_modified_date,
	key_message_lastmodifiedbyid::varchar(18) as key_message_lastmodifiedbyid,
	key_message_systemmodstamp::timestamp_ntz(9) as key_message_systemmodstamp,
	key_message_lastvieweddate::timestamp_ntz(9) as key_message_lastvieweddate,
	key_message_lastreferenceddate::timestamp_ntz(9) as key_message_lastreferenceddate,
	key_message_description::varchar(800) as key_message_description,
	key_message_product::varchar(18) as key_message_product,
	key_message_product_strategy::varchar(18) as key_message_product_strategy,
	key_message_active::number(18,0) as key_message_active,
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
	country_code::varchar(1300) as country_code,
	ap_clm_country::varchar(4) as ap_clm_country,
	key_message_is_shared_resource::number(18,0) as key_message_is_shared_resource,
	key_message_shared_resource::varchar(18) as key_message_shared_resource,
	key_message_recordtypeid::varchar(18) as key_message_recordtypeid,
	key_message_display_order::number(3,0) as key_message_display_order,
	key_message_vexternal_id::varchar(255) as key_message_vexternal_id,
	key_message_cdn_path::varchar(255) as key_message_cdn_path,
	key_message_status::varchar(255) as key_message_status,
	ap_country::varchar(4) as ap_country,
	functional_team::varchar(255) as functional_team,
	janssen_code::varchar(150) as janssen_code,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date,
	vault_document_id::varchar(100) as vault_document_id,
	key_message_group::varchar(4099) as key_message_group,
	key_message_sub_group::varchar(4099) as key_message_sub_group,
	key_message_purpose::varchar(255) as key_message_purpose,
	key_message_content_topic::varchar(255) as key_message_content_topic,
	key_message_content_sub_topic::varchar(255) as key_message_content_sub_topic
	from transformed
)

select * from final 
