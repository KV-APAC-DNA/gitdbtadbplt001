{{ config(materialized='table') }}

with source as (
    select * from {{ source('sgpsdl_raw','sdl_mds_sg_product_hierarchy') }}
),
final as (
  select
    code::varchar(10) as material ,
    name::varchar(100) as material_description ,
    brand_mapping_code::varchar(50) as brand,
    new_category_code::varchar(50) as category,
    product_type_code::varchar(50) as producttype ,
    product_variant_code::varchar(50) as productvarient ,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
  from source
)
select * from final

