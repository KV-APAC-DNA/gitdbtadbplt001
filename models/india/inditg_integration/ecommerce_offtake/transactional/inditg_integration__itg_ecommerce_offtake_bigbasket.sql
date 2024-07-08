{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name = (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_bigbasket') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_bigbasket as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_bigbasket') }}
),
final as
(
    select * from sdl_ecommerce_offtake_bigbasket
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    city_name::varchar(255) as city_name,
    dc_name::varchar(255) as dc_name,
    department::varchar(255) as department,
    top_level_category::varchar(255) as top_level_category,
    lowest_category::varchar(255) as lowest_category,
    product_id::varchar(20) as product_id,
    brand_name::varchar(255) as brand_name,
    product_description::varchar(255) as product_description,
    total_cost_price::float as total_cost_price,
    mrp::float as mrp,
    qty_sold::float as qty_sold,
    total_sales::float as total_sales,
    sub_category::varchar(255) as sub_category,
    manufacturing_company::varchar(255) as manufacturing_company,
    cost_price::float as cost_price
 from final