with sdl_pop6_hk_product_lists_pops as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_product_lists_pops') }}
),
final as (
    select * from sdl_pop6_hk_product_lists_pops
)
select * from final