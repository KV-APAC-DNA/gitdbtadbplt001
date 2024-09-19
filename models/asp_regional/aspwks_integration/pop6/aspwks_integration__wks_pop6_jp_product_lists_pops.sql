with sdl_pop6_jp_product_lists_pops as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_lists_pops') }}
),
final as (
    select * from sdl_pop6_jp_product_lists_pops
)
select * from final