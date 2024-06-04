{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["dstrbtr_grp_cd"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where trim(dstrbtr_grp_cd) in (select distinct trim(distributor_code) from {{ source('idnsdl_raw', 'sdl_mds_id_ref_distributor_group') }});
        {% endif %}"
    )
}}
with source as
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ref_distributor_group') }}
),
final as 
(
    select 
    trim(distributor_code)::varchar(25) as dstrbtr_grp_cd,
    trim(distributor)::varchar(155) as dstrbtr_grp_nm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final