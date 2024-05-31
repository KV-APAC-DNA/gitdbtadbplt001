{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "{% if var('job_to_execute') == 'j_kr_ecomm_offtake_product_master_load' %}
                    delete from {{this}} where (trim(retailer_sku_code),upper(trim(retailer_type))) in (select trim(retailer_sku_code) as retailer_sku_code, upper(trim(name)) as retailer_type from source('ntasdl_raw', 'sdl_mds_kr_ecom_offtake_product_mapping') );
                    {% elif var('job_to_execute') == 'j_kr_generic_query_execution' %}
                    delete from {{this}};
                    {% endif %}"
    )
}}


with sdl_mds_kr_ecom_offtake_product_mapping as
(
    select * from {{ source('ntasdl_raw', 'sdl_mds_kr_ecom_offtake_product_mapping')}}
),
 
sdl_kr_ecommerce_offtake_product_master as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_offtake_product_master')}}
),

{% if var("job_to_execute") == 'j_kr_ecomm_offtake_product_master_load' %}

final as
(
    select
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as load_date,
        'MDS'::varchar(255) as source_file_name,
        trim(retailer_sku_code)::varchar(100) as retailer_sku_code,
        trim(jnj_sku_code)::varchar(255) as jnj_sku_code,
        trim(name)::varchar(255) as retailer_type,
        trim(ean)::varchar(255) as ean,
        trim(ean)::varchar(255) as retailer_barcode,
        trim(brand)::varchar(255) as brand,
        trim(retailer_sku_name)::varchar(2000) as retailer_sku_name
    from sdl_mds_kr_ecom_offtake_product_mapping
)
select * from final

{% elif var("job_to_execute") == 'j_kr_generic_query_execution' %}

final as
(
    select
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as load_date,
        'MDS'::varchar(255) as source_file_name,
        trim(retailer_sku_code)::varchar(100) as retailer_sku_code,
        trim(jnj_sku_code)::varchar(255) as jnj_sku_code,
        trim(name)::varchar(255) as retailer_type,
        trim(ean)::varchar(255) as ean,
        trim(ean)::varchar(255) as retailer_barcode,
        trim(brand)::varchar(255) as brand,
        trim(retailer_sku_name)::varchar(2000) as retailer_sku_name
    from sdl_kr_ecommerce_offtake_product_master
)
select * from final

{% endif %}