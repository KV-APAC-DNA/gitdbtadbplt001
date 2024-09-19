with sdl_pop6_jp_service_levels as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_service_levels') }}
),
final as (
    select * from sdl_pop6_jp_service_levels
)
select * from final