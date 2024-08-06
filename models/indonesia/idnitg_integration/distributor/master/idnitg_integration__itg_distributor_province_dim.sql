{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["code"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (trim(code)) in (select distinct trim(code) from {{ source('idnsdl_raw', 'sdl_mds_id_ref_province') }});
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
    trim(code)::varchar(10) as code,
	trim(name)::varchar(50) as name,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final