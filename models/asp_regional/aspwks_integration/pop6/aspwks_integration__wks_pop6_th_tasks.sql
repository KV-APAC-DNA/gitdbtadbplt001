with sdl_pop6_th_tasks as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_tasks') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_tasks__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_tasks__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_th_tasks
)
select * from final