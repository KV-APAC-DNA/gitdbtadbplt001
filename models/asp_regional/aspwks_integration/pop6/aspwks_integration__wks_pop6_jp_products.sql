with sdl_pop6_jp_products as (
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_products') }}
),
final as (
    select * from sdl_pop6_jp_products
)
select * from final