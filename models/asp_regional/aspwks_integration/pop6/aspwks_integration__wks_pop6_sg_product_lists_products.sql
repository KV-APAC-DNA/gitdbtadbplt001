with sdl_pop6_sg_product_lists_products as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_lists_products') }}
),
final as (
    select * from sdl_pop6_sg_product_lists_products
)
select * from final