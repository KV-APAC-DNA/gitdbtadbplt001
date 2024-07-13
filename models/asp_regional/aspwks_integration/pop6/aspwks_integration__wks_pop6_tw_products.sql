with sdl_pop6_tw_products as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_products') }}
),
final as (
    select * from sdl_pop6_tw_products
)
select * from final