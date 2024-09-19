with sdl_pop6_jp_tasks as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_tasks') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_tasks__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_tasks__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_jp_tasks
)
select * from final