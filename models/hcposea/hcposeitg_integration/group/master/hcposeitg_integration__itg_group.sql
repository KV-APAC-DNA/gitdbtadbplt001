{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (group_id) in (select group_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_group') }} stg_osea_group
                where stg_osea_group.group_id = group_id);
                {% endif %}"
    )
}}
with sdl_hcp_osea_group
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_group') }}
)
,
transformed as
(
    select
    GROUP_ID,
NAME,
DEVELOPER_NAME,
RELATED_ID,
TYPE,
EMAIL,
OWNER_ID,
SEND_EMAIL_TO_MEMBERS,
INCUDE_BOSSES,
CREATEDDATE,
CREATEDBYID,
LAST_MODIFIED_DATE,
LASTMODIFIEDBYID,
SYSTEMMODSTAMP,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_group
)
,
final as
(select 
	GROUP_ID::VARCHAR(18) AS GROUP_ID,
NAME::VARCHAR(50) AS NAME,
DEVELOPER_NAME::VARCHAR(100) AS DEVELOPER_NAME,
RELATED_ID::VARCHAR(80) AS RELATED_ID,
TYPE::VARCHAR(100) AS TYPE,
EMAIL::VARCHAR(40) AS EMAIL,
OWNER_ID::VARCHAR(30) AS OWNER_ID,
SEND_EMAIL_TO_MEMBERS::VARCHAR(10) AS SEND_EMAIL_TO_MEMBERS,
INCUDE_BOSSES::VARCHAR(10) AS INCUDE_BOSSES,
CREATEDDATE::TIMESTAMP_NTZ(9) AS CREATEDDATE,
CREATEDBYID::VARCHAR(18) AS CREATEDBYID,
LAST_MODIFIED_DATE::TIMESTAMP_NTZ(9) AS LAST_MODIFIED_DATE,
LASTMODIFIEDBYID::VARCHAR(18) AS LASTMODIFIEDBYID,
SYSTEMMODSTAMP::TIMESTAMP_NTZ(9) AS SYSTEMMODSTAMP,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
	from transformed
)

select * from final 
