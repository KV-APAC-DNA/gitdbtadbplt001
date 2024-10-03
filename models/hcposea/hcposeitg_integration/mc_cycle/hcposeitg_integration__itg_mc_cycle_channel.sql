{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (mc_cycle_channel_id) in (select mc_cycle_channel_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_mc_cycle_channel') }} stg_mc_cycle_channel
                where stg_mc_cycle_channel.mc_cycle_channel_id = mc_cycle_channel_id);
                {% endif %}"
    )
}}

with sdl_hcp_osea_mc_cycle_channel
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_mc_cycle_channel') }}
)
,
transformed as
(
select 
mc_cycle_channel_id,
       (CASE WHEN UPPER(IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(IS_DELETED) IS NULL THEN 0 WHEN UPPER(IS_DELETED) = ' ' THEN 0 ELSE 0 END) AS IS_DELETED,
cycle_channel_name,
record_type_id,
created_date,
created_by_id,
last_modified_date,
last_modified_by_id,
system_modstamp,
may_edit,
       CASE WHEN UPPER(IS_LOCKED) = 'TRUE' THEN 1 WHEN UPPER(IS_LOCKED) = 'FALSE' THEN 0 ELSE 0 END AS IS_LOCKED,	          
cycle,
channel_criteria,
channel_label,
channel_object,
channel_weight,
external_id,
veeva_external_id,
master_align_id,
country_code,
legacy_id,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_mc_cycle_channel
)
,
final as
(select 
mc_cycle_channel_id::varchar(18) AS mc_cycle_channel_id,
IS_DELETED::NUMBER(38,0) as IS_DELETED,
cycle_channel_name::varchar(255) AS cycle_channel_name,
record_type_id::varchar(18) AS record_type_id,
created_date::timestamp_ntz(9) AS created_date,
created_by_id::varchar(18) AS created_by_id,
last_modified_date::timestamp_ntz(9) AS last_modified_date,
last_modified_by_id::varchar(18) AS last_modified_by_id,
system_modstamp::timestamp_ntz(9) AS system_modstamp,
may_edit::varchar(10) AS may_edit,
IS_LOCKED::NUMBER(38,0) as IS_LOCKED,
cycle::varchar(20) AS cycle,
channel_criteria::varchar(10000) AS channel_criteria,
channel_label::varchar(100) AS channel_label,
channel_object::varchar(50) AS channel_object,
channel_weight::number(32) AS channel_weight,
external_id::varchar(100) AS external_id,
veeva_external_id::varchar(100) AS veeva_external_id,
master_align_id::varchar(36) AS master_align_id,
country_code::varchar(2) AS country_code,
legacy_id::varchar(255) AS legacy_id,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date

	from transformed
)

select * from final 
