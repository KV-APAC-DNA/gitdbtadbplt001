{{
    config
    (
        materialized = "incremental",
        incremental_strategy = "append",
        pre_hook ="{% if is_incremental() %}
        delete from {{this}} 
        where source_file_name in (select distinct source_file_name from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_flipkart') }}) ;
        {% endif %}"
    )
}}

with sdl_ecommerce_offtake_flipkart as
(
    select * from {{ source('indsdl_raw', 'sdl_ecommerce_offtake_flipkart') }}
),
final as
(
    select * from sdl_ecommerce_offtake_flipkart
)
select load_date::timestamp_ntz(9) as load_date,
    source_file_name::varchar(255) as source_file_name,
    account_name::varchar(255) as account_name,
    transaction_date::date as transaction_date,
    fsn::varchar(50) as fsn,
    product_description::varchar(255) as product_description,
    brand::varchar(50) as brand,
    qty::float as qty,
    gmv::float as gmv
 from final