with sdl_pop6_sg_tasks as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_tasks') }}
    where file_name not in (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_tasks__null_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_sg_tasks__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_sg_tasks
)
select * from final