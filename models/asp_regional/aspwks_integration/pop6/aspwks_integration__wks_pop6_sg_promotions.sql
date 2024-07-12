with sdl_pop6_sg_promotions as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_promotions') }}
),
final as (
    select * from sdl_pop6_sg_promotions
)
select * from final