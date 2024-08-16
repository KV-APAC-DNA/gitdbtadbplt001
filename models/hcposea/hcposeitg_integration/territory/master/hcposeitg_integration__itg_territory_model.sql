
{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if is_incremental() %}
                delete from {{this}} 
where territory_model_source_id  in(select territory_model_source_id from hcp_osea_sdl.sdl_hcp_osea_territory_model);
                    {% endif %}"
    )
}}

with sdl_hcp_osea_territory_model
as
(
select * from dev_dna_load.hcposesdl_raw.sdl_hcp_osea_territory_model
)
,
transformed
as
(
select territory_model_source_id,
       (case when upper(is_deleted) = 'true' then 1 when upper(is_deleted) is null then 0 when upper(is_deleted) = ' ' then 0 else 0 end) as is_deleted,
       name,
       created_date,
       created_by_id,
       last_modified_date,
       last_modified_by_id,
       system_mod_stamp,
       (case when upper(may_edit) = 'true' then 1 when upper(may_edit) is null then 0 when upper(may_edit) = ' ' then 0 else 0 end) as may_edit,
       (case when upper(is_locked) = 'true' then 1 when upper(is_locked) is null then 0 when upper(is_locked) = ' ' then 0 else 0 end) as is_locked,
       description,
       activated_date,
       deactivated_date,
       state,
       developer_name,
       last_run_rules_end_date,
       (case when upper(is_clone_source) = 'true' then 1 when upper(is_clone_source) is null then 0 when upper(is_clone_source) = ' ' then 0 else 0 end) as is_clone_source,
       last_opp_terr_assign_end_date,
       '' as country_code,
       current_timestamp() as inserted_date ,
       current_timestamp() as updated_date
       
from sdl_hcp_osea_territory_model
)

,
final as
(select 
   
	territory_model_source_id::varchar(18)  as territory_model_source_id,
	is_deleted::varchar(5) as is_deleted,
	name::varchar(80) as name,
	created_date::timestamp_ntz(9) as created_date,
	created_by_id::varchar(18) as created_by_id,
	last_modified_date::timestamp_ntz(9) as last_modified_date,
	last_modified_by_id::varchar(18) as last_modified_by_id,
	system_mod_stamp::timestamp_ntz(9) as system_mod_stamp,
	may_edit::varchar(5) as may_edit,
	is_locked::varchar(5) as is_locked,
	description::varchar(255) as description,
	activated_date::timestamp_ntz(9) as activated_date,
	deactivated_date::timestamp_ntz(9) as deactivated_date,
	state::varchar(255) as state,
	developer_name::varchar(80) as developer_name,
	last_run_rules_end_date::timestamp_ntz(9) as last_run_rules_end_date,
	is_clone_source::varchar(5) as is_clone_source,
	last_opp_terr_assign_end_date::timestamp_ntz(9) as last_opp_terr_assign_end_date,
	country_code::varchar(10) as country_code,
	inserted_date::timestamp_ntz(9) as inserted_date,
	updated_date::timestamp_ntz(9) as updated_date
    from transformed 
)

select * from final 