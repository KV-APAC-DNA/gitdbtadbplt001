with sdl_pop6_jp_tasks as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_tasks') }}
),
final as (
    select * from sdl_pop6_jp_tasks
)
select * from final