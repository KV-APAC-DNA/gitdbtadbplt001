with sdl_pop6_tw_tasks as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_tasks') }}
),
final as (
    select * from sdl_pop6_tw_tasks
)
select * from final