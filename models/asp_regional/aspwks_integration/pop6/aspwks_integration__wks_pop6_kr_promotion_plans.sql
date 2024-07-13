with sdl_pop6_kr_promotion_plans as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_promotion_plans') }}
),
final as (
    select * from sdl_pop6_kr_promotion_plans
)
select * from final