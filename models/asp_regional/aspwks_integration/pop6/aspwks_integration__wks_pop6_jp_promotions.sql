with sdl_pop6_jp_promotions as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_promotions') }}
),
final as (
    select * from sdl_pop6_jp_promotions
)
select * from final