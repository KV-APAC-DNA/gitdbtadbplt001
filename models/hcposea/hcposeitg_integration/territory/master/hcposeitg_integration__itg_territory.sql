{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        pre_hook="
        {% if is_incremental() %}
            DELETE FROM {{ this }} WHERE territory_source_id IN (
            SELECT territory_source_id FROM {{ source('hcposesdl_raw', 'sdl_hcp_osea_territory') }})
        {% endif %}"
    )
}}

with sdl_hcp_osea_territory as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_territory') }}
),

sdl_hcp_osea_territory_model as (
    select * from {{ source('hcposesdl_raw', 'sdl_hcp_osea_territory_model') }}
),

result as (
select t.territory_source_id,
  t.territory_name,
  t.territory_type_source_id,
  t.territory_model_source_id,
  t.parent_territory_source_id,
  t.description,
  t.account_access_level,
  t.contact_access_level,
  t.last_modified_date,
  t.last_modified_by_id,
  t.system_mod_stamp,
  t.developer_name,
  t.parent_territory1_source_id,
  t.territory1_source_id,
  '' as country_code,
  current_timestamp() as inserted_date,
  current_timestamp() as updated_date
from sdl_hcp_osea_territory t
left join sdl_hcp_osea_territory_model tm on t.territory_model_source_id = tm.territory_model_source_id
where upper(tm.state) = 'ACTIVE'
  and t.parent_territory_source_id is not null
  ),

final as (
    select 
        territory_source_id::varchar(18) as territory_source_id,
        territory_name::varchar(80) as territory_name,
        territory_type_source_id::varchar(18) as territory_type_source_id,
        territory_model_source_id::varchar(18) as territory_model_source_id,
        parent_territory_source_id::varchar(18) as parent_territory_source_id,
        description::varchar(1000) as description,
        account_access_level::varchar(40) as account_access_level,
        contact_access_level::varchar(40) as contact_access_level,
        last_modified_date::timestamp_ntz(9) as last_modified_date,
        last_modified_by_id::varchar(18) as last_modified_by_id,
        system_mod_stamp::timestamp_ntz(9) as system_mod_stamp,
        developer_name::varchar(80) as developer_name,
        parent_territory1_source_id::varchar(18) as parent_territory1_source_id,
        territory1_source_id::varchar(18) as territory1_source_id,
        country_code::varchar(255) as country_code,
        current_timestamp()::timestamp_ntz(9) as inserted_date,
        current_timestamp()::timestamp_ntz(9) as updated_date
    from result
)

select * from final