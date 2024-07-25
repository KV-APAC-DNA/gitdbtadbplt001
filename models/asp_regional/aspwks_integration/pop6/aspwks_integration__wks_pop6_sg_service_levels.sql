with sdl_pop6_sg_service_levels as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_service_levels') }}
),
final as (
    select * from sdl_pop6_sg_service_levels
)
select * from final