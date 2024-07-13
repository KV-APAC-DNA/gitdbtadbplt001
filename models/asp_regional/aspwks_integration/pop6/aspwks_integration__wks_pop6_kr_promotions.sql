with sdl_pop6_kr_promotions as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_promotions') }}
),
final as (
    select * from sdl_pop6_kr_promotions
)
select * from final