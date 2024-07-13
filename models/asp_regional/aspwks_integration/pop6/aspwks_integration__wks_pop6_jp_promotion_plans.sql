with sdl_pop6_jp_promotion_plans as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_promotion_plans') }}
),
final as (
    select * from sdl_pop6_jp_promotion_plans
)
select * from final