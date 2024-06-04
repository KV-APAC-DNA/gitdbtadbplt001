{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where 0 != (select count(*)
        FROM {{ source('idnsdl_raw', 'sdl_mds_id_pos_cust_prod_mapping') }});
        {% endif %}"
    )
}}
with source as 
(
    select * from {{ source('idnsdl_raw', 'sdl_mds_id_pos_cust_prod_mapping') }}
),
final as
(
    select 
    trim(account)::varchar(25) as account,
	trim(plu_sku_desc)::varchar(200) as plu_sku_desc,
	trim(jj_sap_code)::varchar(50) as jj_sap_prod_id,
	trim(brand)::varchar(50) as brand,
	trim(brand_2)::varchar(50) as brand2,
	trim(sku_sales_cube)::varchar(200) as sku_sales_cube,
	trim(sku_sap_codename)::varchar(200) as sku_sap_codename,
	convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final