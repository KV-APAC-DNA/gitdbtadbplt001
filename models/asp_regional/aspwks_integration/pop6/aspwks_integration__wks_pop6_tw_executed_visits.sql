with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_executed_visits') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_tw_executed_visits__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_tw_executed_visits__duplicate_test') }}
    )
),
final as
(
    select * from source
)
select * from final