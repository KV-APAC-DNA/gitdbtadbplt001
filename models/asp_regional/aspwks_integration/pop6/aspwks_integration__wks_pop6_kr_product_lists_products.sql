with sdl_pop6_hk_product_lists_products as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_product_lists_products') }}
),
final as (
    select * from sdl_pop6_hk_product_lists_products
)
select * from final