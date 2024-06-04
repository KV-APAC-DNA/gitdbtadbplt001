{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["prov_id"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (trim(prov_id)) in (select distinct trim(prov_id) from {{ source('idnsdl_raw', 'sdl_mds_id_ref_province') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ref_province') }}
),
final as 
(
    select
    trim(prov_id)::varchar(10) as prov_id,
	trim(province)::varchar(50) as prov_nm,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final