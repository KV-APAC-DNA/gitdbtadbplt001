with sdl_pop6_th_products as (
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_products') }}
),
final as (
    select * from sdl_pop6_th_products
)
select * from final