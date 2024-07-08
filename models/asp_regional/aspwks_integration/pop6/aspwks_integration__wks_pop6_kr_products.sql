with sdl_pop6_kr_products as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_products') }}
),
final as (
    select * from sdl_pop6_kr_products
)
select * from final