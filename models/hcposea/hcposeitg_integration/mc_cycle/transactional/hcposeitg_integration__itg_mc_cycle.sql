{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}}
                where (mc_cycle_id) in (select mc_cycle_id
                from {{ source('hcposesdl_raw', 'sdl_hcp_osea_mc_cycle') }} stg_mc_cycle
                where stg_mc_cycle.mc_cycle_id = mc_cycle_id);
                {% endif %}"
    )
}}

with sdl_hcp_osea_mc_cycle
as
(
select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_mc_cycle') }}
)
,
transformed as
(
select mc_cycle_id,
owner_id,
(CASE WHEN UPPER(IS_DELETED) = 'TRUE' THEN 1 WHEN UPPER(IS_DELETED) IS NULL THEN 0 WHEN UPPER(IS_DELETED) = ' ' THEN 0 ELSE 0 END) AS IS_DELETED,
cycle_name,
record_type_id,
created_date,
created_by_id,
last_modified_date,
last_modified_by_id,
system_modstamp,
may_edit,
 CASE WHEN UPPER(IS_LOCKED) = 'TRUE' THEN 1 WHEN UPPER(IS_LOCKED) = 'FALSE' THEN 0 ELSE 0 END AS IS_LOCKED,	          
last_viewed_date,
last_referenced_date,
description,
end_date,
external_id,
over_reached_threshold,
start_date,
status,
under_reached_threshold,
calculate_pull_through,
activate_edited_goals,
goal_editing_rules,
master_align_id,
country_code,
legacy_id,
current_timestamp() as INSERTED_DATE,
       NULL as UPDATED_DATE 
from sdl_hcp_osea_mc_cycle
)
,
final as
(select 
	mc_cycle_id::varchar(18) AS mc_cycle_id,
owner_id::varchar(40) AS owner_id,
IS_DELETED::NUMBER(38,0) as IS_DELETED,
cycle_name::varchar(255) AS cycle_name,
record_type_id::varchar(18) AS record_type_id,
created_date::timestamp_ntz(9) AS created_date,
created_by_id::varchar(18) AS created_by_id,
last_modified_date::timestamp_ntz(9) AS last_modified_date,
last_modified_by_id::varchar(18) AS last_modified_by_id,
system_modstamp::timestamp_ntz(9) AS system_modstamp,
may_edit::varchar(10) AS may_edit,
IS_LOCKED::NUMBER(38,0) as IS_LOCKED,
last_viewed_date::timestamp_ntz(9) AS last_viewed_date,
last_referenced_date::timestamp_ntz(9) AS last_referenced_date,
description::varchar(1000) AS description,
end_date::timestamp_ntz(9) AS end_date,
external_id::varchar(100) AS external_id,
over_reached_threshold::number(15,1) AS over_reached_threshold,
start_date::timestamp_ntz(9) AS start_date,
status::varchar(100) AS status,
under_reached_threshold::number(15,1) AS under_reached_threshold,
calculate_pull_through::varchar(10) AS calculate_pull_through,
activate_edited_goals::varchar(10) AS activate_edited_goals,
goal_editing_rules::varchar(100) AS goal_editing_rules,
master_align_id::varchar(36) AS master_align_id,
country_code::varchar(2) AS country_code,
legacy_id::varchar(255) AS legacy_id,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date

	from transformed
)

select * from final 
