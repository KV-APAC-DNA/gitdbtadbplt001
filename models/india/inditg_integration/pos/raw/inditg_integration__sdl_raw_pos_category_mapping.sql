{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}
with sdl_pos_category_mapping as 
(
    select * from {{ source('indsdl_raw', 'sdl_pos_category_mapping') }}
),
final as
 (
    select
        account_name::varchar(255) as account_name,
	    article_cd::varchar(20) as article_cd,
	    article_desc::varchar(255) as article_desc,
	    ean::varchar(20) as ean,
	    sap_cd::varchar(20) as sap_cd,
	    mother_sku_name::varchar(255) as mother_sku_name,
	    brand_name::varchar(255) as brand_name,
	    franchise_name::varchar(255) as franchise_name,
	    product_category_name::varchar(255) as product_category_name,
	    variant_name::varchar(255) as variant_name,
	    product_name::varchar(255) as product_name,
	    internal_category::varchar(255) as internal_category,
	    internal_sub_category::varchar(255) as internal_sub_category,
	    external_category::varchar(255) as external_category,
	    external_sub_category::varchar(255) as external_sub_category,
	    filename::varchar(100) as filename,
	    run_id::number(14,0) as run_id,
	    crt_dttm::timestamp_ntz(9) as crt_dttm
    from sdl_pos_category_mapping
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
     
 )
select * from final