with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_pop_lists') }}
    where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_pop_lists__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_pop_lists__duplicate_test') }}
    )
),
final as
(
    select * from source
)
select * from final