with sdl_pop6_sg_tasks as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_tasks') }}
),
final as (
    select * from sdl_pop6_sg_tasks
)
select * from final