{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name in (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_firstcry') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_firstcry as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_firstcry') }}
),
final as
(
    select * from sdl_ecommerce_offtake_firstcry where brand_name not like '%Grand Total%'
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    brand_name::varchar(255) as brand_name,
    product_id::varchar(20) as product_id,
    product_name::varchar(65535) as product_name,
    sum_of_sales::float as sum_of_sales,
    sum_of_mrpsales::float as sum_of_mrpsales,
    file_name:: varchar(255) as file_name
 from final