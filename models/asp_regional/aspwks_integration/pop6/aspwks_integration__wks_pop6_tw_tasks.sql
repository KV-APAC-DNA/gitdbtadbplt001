with sdl_pop6_tw_tasks as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_tasks') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_tw_tasks__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_tw_tasks__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_tw_tasks
)
select * from final