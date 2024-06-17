{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["jj_sap_dstrbtr_id"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where trim(jj_sap_dstrbtr_id) in (select distinct trim(customer_id) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_customer_hierarchy') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_customer_hierarchy') }}
),
final as 
(
    select
       trim(distributor_group)::varchar(20) as dstrbtr_grp_cd,
       trim(distributor_id)::varchar(20) as dstrbtr_id,
       trim(customer_id)::varchar(20) as jj_sap_dstrbtr_id,
       trim(customer_name)::varchar(50) as jj_sap_dstrbtr_nm,
       trim(city)::varchar(100) as city,
       trim(area)::varchar(50) as area,
       trim(region)::varchar(50) as region,
       trim(bdm)::varchar(50) as bdm_nm,
       trim(rbm)::varchar(50) as rbm_nm,
       trim(status)::varchar(50) as status,
       trim(lead_time)::number(18,0) as lead_tm,
       trim(prov_id)::varchar(50) as prvnce_id,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as uptd_dttm,
	   nvl(effective_from,'200001')::varchar(10) as effective_from,
       nvl(effective_to,'999912')::varchar(10) as effective_to 
    from source
)
select * from final