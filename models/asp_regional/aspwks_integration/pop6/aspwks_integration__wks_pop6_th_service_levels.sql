with sdl_pop6_th_service_levels as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_service_levels') }}
),
final as (
    select * from sdl_pop6_th_service_levels
)
select * from final