with sdl_pop6_hk_tasks as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_tasks') }}
        where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_tasks__duplicate_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_tasks__null_test') }}
    )
),
final as (
    select * from sdl_pop6_hk_tasks
)
select * from final