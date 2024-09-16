with sdl_pop6_th_tasks as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_tasks') }}
    
),
final as (
    select * from sdl_pop6_th_tasks
)
select * from final