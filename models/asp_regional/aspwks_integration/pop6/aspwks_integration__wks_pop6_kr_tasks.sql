with sdl_pop6_kr_tasks as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_tasks') }}
),
final as (
    select * from sdl_pop6_kr_tasks
)
select * from final