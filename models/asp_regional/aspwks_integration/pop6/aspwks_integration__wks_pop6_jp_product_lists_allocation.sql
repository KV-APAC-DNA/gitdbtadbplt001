with sdl_pop6_jp_product_lists_allocation as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_jp_product_lists_allocation') }}
),
final as (
    select * from sdl_pop6_jp_product_lists_allocation
)
select * from final