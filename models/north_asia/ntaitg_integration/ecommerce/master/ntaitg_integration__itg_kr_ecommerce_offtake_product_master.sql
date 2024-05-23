{{
    config(
        materialized= "incremental",
        incremental_strategy= "append",
        pre_hook = "delete from {{this}} 
                    where (trim(retailer_sku_code),upper(trim(retailer_type))) 
                    in (select trim(retailer_sku_code) as retailer_sku_code, upper(trim(name)) as retailer_type 
                    from dev_dna_load.snapaspsdl_raw.sdl_mds_kr_ecom_offtake_product_mapping )"
    )
}}


with source as
(
    --select * from {{ source('ntasdl_raw', 'sdl_mds_kr_ecom_offtake_product_mapping')}} (add to prehook as well)
    select * from dev_dna_load.snapaspsdl_raw.sdl_mds_kr_ecom_offtake_product_mapping
),
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
    from source
)
select * from final