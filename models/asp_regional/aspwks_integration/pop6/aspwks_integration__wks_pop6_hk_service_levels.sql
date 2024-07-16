with sdl_pop6_hk_service_levels as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_service_levels') }}
),
final as (
    select * from sdl_pop6_hk_service_levels
)
select * from final