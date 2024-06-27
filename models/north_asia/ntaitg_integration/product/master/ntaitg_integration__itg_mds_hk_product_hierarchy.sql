{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["sap_brand","sap_base_product"]
    )
}}

with source as (
    select * from {{ source('ntasdl_raw', 'sdl_mds_hk_product_hierarchy') }}
),
final as (
    select 
        sap_brand::varchar(200) as sap_brand,
        sap_base_product::varchar(200) as sap_base_product,
        hk_brand_code::varchar(500) as hk_brand_code,
        hk_base_product_code::varchar(500) as hk_base_product_code,
        hk_base_product_name::varchar(500) as hk_base_product_name
    from source
)
select * from final
