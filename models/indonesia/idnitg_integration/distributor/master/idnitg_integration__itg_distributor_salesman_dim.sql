{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["jj_sap_dstrbtr_id","slsmn_id"],
        pre_hook = "delete from {{this}} where (trim(jj_sap_dstrbtr_id)||trim(slsmn_id)) in (select distinct trim(sales_office_id_jnj)||trim(salesman_id)
                          from {{ source('idnsdl_raw', 'sdl_mds_id_ref_salesman') }});"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ref_salesman') }}
),
final as 
(
    select
    trim(sales_office_id_jnj)::varchar(20) as jj_sap_dstrbtr_id,
	trim(sales_office)::varchar(200) as jj_sap_dstrbtr_nm,
	trim(salesman_id)::varchar(50) as slsmn_id,
	trim(salesman)::varchar(100) as slsmn_nm,
	trim(key_field)::varchar(70) as rec_key,
	current_timestamp()::timestamp_ntz(9) as crtd_dttm,
	current_timestamp()::timestamp_ntz(9) as updt_dttm,
	trim(sfa_id)::varchar(255) as sfa_id,
	effective_from::varchar(10) as effective_from,
	nvl(effective_to,'999912')::varchar(10) as effective_to
    from source
)
select * from final