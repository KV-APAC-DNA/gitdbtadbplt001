with sdl_pop6_sg_product_lists_pops as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_lists_pops') }}
),
final as (
    select * from sdl_pop6_sg_product_lists_pops
)
select * from final