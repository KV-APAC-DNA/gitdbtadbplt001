with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_offtake_sales_ebay') }}
),
final as (
    select * from source
)
select * from final