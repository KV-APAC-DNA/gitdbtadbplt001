{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["prod_key"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where (trim(prod_key)) in (select distinct trim(product_key) from {{ source('idnsdl_raw', 'sdl_mds_id_ref_mapping_product') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_ref_mapping_product') }}
),
final as 
(
    select
       trim(distgroup_code)::varchar(15) as dstrbtr_grp_cd,
       trim(customer_id)::varchar(50) as dstrbtr_id,
       trim(distproduct_id)::varchar(50) as dstrbtr_prod_id,
       trim(sap_code)::varchar(50) as jj_sap_prod_id,
       trim(description)::varchar(255) as jj_sap_prod_desc,
       trim(franchise)::varchar(50) as franchise,
       trim(brand)::varchar(25) as brand,
       cast(uom as numeric) as cse,
       trim(product_key)::varchar(100) as prod_key,
       current_timestamp()::timestamp_ntz(9) as crtd_dttm,
       current_timestamp()::timestamp_ntz(9) as updt_dttm,
       case
         when denominator is null then 1
         when denominator = 0 then 1
         else cast(denominator as numeric)
       end::number(20,0) as denominator,
	   nvl(effective_from,'200001')::varchar(10) as effective_from,
       nvl(effective_to,'999912')::varchar(10) as effective_to
    from source
)
select * from final