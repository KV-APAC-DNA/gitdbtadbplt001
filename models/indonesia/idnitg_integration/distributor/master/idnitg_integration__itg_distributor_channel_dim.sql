{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["dstrbtr_grp_cd","dstrbtr_chnl_type_id"],
        pre_hook = " {% if is_incremental() %}
        delete from {{this}} where (trim(dstrbtr_grp_cd)||trim(dstrbtr_chnl_type_id)) in (select distinct trim(dist_group)||trim(type_dist_id) from {{ source('idnsdl_raw', 'sdl_mds_id_ref_mapping_channel_adftemp') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ref_mapping_channel_adftemp') }}
),
final as 
(
    select
    trim(dist_group)::varchar(75) as dstrbtr_grp_cd,
	trim(type_dist_id)::varchar(75) as dstrbtr_chnl_type_id,
	trim(type_dist)::varchar(75) as dstrbtr_chnl_type_nm,
	trim(type_jnj)::varchar(75) as jnj_chnl_type_id,
	trim(type_jnj_id)::varchar(50) as jnj_chnl_type_nm,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm,
	nvl(effective_from,'200001')::varchar(10) as effective_from,
    nvl(effective_to,'999912')::varchar(10) as effective_to 
    from source
)
select * from final