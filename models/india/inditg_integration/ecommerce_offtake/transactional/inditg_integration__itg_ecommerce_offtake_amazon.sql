{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name in (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_amazon') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_amazon as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_amazon') }}
),
final as
(
    select * from sdl_ecommerce_offtake_amazon
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    platform::varchar(255) as platform,
    brand::varchar(255) as brand,
    rpc::varchar(20) as rpc,
    product_title::varchar(500) as product_title,
    quantity::number(18,0) as quantity,
    mrp::float as mrp,
    mrp_offtakes_value::float as mrp_offtakes_value,
    month::varchar(20) as month,
    file_name:: varchar(255) as file_name
 from final