with sdl_pop6_tw_promotions as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_promotions') }}
),
final as (
    select * from sdl_pop6_tw_promotions
)
select * from final