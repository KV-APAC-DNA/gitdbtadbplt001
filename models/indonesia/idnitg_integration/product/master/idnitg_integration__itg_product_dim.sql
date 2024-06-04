{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["jj_sap_prod_id"],
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where TRIM(jj_sap_prod_id) in (
        select DISTINCT TRIM(sap_code) from {{ source('idnsdl_raw', 'sdl_mds_id_lav_product_hierarchy') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_lav_product_hierarchy') }}
),
final as 
(
    select
       trim(sap_code)::varchar(50) as jj_sap_prod_id,
       trim(sku_description)::varchar(100) as jj_sap_prod_desc,
       trim(franchise)::varchar(50) as franchise,
       trim(brand)::varchar(50) as brand,
       trim(variant1)::varchar(50) as variant1,
       trim(variant2)::varchar(50) as variant2,
       trim(variant3)::varchar(50) as variant3,
       trim(status)::varchar(50) as status,
       cast(put_up as integer) as put_up,
       cast(uom as decimal) as uom,
       trim(sap_upgrade)::varchar(50) as jj_sap_upgrd_prod_id,
       trim(sku_description2)::varchar(100) as jj_sap_upgrd_prod_desc,
       cast(replace(price,',','') as decimal) as price,
       cast(sku_class as integer) as prod_class,
       trim(sap_code_mapping)::varchar(50) as jj_sap_cd_mp_prod_id,
       trim(sku_description3)::varchar(100) as jj_sap_cd_mp_prod_desc,
       null::number(18,0) as price_vmr,
       null::varchar(60) as pft_sm,
       null::varchar(60) as pft_mm,
       null::varchar(60) as pft_ws,
       null::varchar(60) as pft_prov,
       null::varchar(60) as pft_ds,
       null::varchar(60) as pft_mws,
       null::varchar(60) as pft_apt,
       null::varchar(60) as pft_bs,
       null::varchar(60) as pft_cs,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS crtd_dttm,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS uptd_dttm,
	   nvl(effective_from,'200001')::varchar(10) as effective_from,
       nvl(effective_to,'999912')::varchar(10) as effective_to 
    from source
)
select * from final