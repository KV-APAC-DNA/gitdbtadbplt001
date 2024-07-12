with sdl_pop6_jp_product_lists_products as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_lists_products') }}
),
final as (
    select * from sdl_pop6_jp_product_lists_products
)
select * from final