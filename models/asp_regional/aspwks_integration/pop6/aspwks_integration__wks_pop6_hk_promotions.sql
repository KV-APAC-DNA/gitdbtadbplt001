with sdl_pop6_hk_promotions as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_promotions') }}
),
final as (
    select * from sdl_pop6_hk_promotions
)
select * from final