{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name = (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_grofers') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_grofers as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_grofers') }}
),
final as
(
    select * from sdl_ecommerce_offtake_grofers
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    l_cat::varchar(255) as l_cat,
    l1_cat::varchar(255) as l1_cat,
    product_id::varchar(255) as product_id,
    product_name::varchar(65535) as product_name,
    sum_of_mrp_gmv::float as sum_of_mrp_gmv,
    sum_of_qty_sold::float as sum_of_qty_sold
 from final