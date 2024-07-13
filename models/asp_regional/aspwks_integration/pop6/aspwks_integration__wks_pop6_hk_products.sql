with sdl_pop6_hk_products as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_products') }}
),
final as (
    select * from sdl_pop6_hk_products
)
select * from final