with 
source as
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_planned_visits') }}
),
final as
(
    select * from source
)
select * from final