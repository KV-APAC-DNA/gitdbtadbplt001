with sdl_pop6_th_promotions as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_promotions') }}
),
final as (
    select * from sdl_pop6_th_promotions
)
select * from final