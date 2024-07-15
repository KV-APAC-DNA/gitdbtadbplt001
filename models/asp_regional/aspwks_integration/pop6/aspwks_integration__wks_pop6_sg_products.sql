with sdl_pop6_sg_products as (
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_products') }}
),
final as (
    select * from sdl_pop6_sg_products
)
select * from final