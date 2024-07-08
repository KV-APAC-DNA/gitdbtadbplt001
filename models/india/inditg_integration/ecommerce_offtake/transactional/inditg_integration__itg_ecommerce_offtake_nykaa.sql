{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name = (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_nykaa') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_nykaa as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_nykaa') }}
),
final as
(
    select * from sdl_ecommerce_offtake_nykaa where sku_code not like '%Grand Total%'
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    sku_code::varchar(255) as sku_code,
    qty::float as qty,
    mrp::float as mrp,
    product_name::varchar(65535) as product_name
 from final