with sdl_pop6_th_promotion_plans as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_promotion_plans') }}
    
),
final as (
    select * from sdl_pop6_th_promotion_plans
)
select * from final