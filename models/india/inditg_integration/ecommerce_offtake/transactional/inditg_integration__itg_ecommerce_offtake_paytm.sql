{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name = (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_paytm') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_paytm as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_paytm') }}
),
final as
(
    select * from sdl_ecommerce_offtake_paytm
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    date::timestamp_ntz(9) as date,
    reporting_category::varchar(255) as reporting_category,
    product_id::varchar(20) as product_id,
    product_name::varchar(65535) as product_name,
    brand_id::varchar(20) as brand_id,
    brand_name::varchar(255) as brand_name,
    merchant_id::varchar(20) as merchant_id,
    merchant_name::varchar(255) as merchant_name,
    l3::varchar(255) as l3,
    product_category::varchar(2000) as product_category,
    category_id::varchar(20) as category_id,
    quantity_ordered::float as quantity_ordered,
    gmv_ordered::float as gmv_ordered
 from final