with sdl_pop6_hk_product_lists_allocation as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_product_lists_allocation') }}
),
final as (
    select * from sdl_pop6_hk_product_lists_allocation
)
select * from final