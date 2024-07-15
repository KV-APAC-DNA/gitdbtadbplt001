with sdl_pop6_hk_tasks as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_tasks') }}
),
final as (
    select * from sdl_pop6_hk_tasks
)
select * from final