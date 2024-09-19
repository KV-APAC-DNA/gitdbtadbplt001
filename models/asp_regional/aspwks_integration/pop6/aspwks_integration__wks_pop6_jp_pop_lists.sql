with 
source as
(
    select * from {{source('jpnsdl_raw','sdl_pop6_jp_pop_lists')}}
     where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_pop_lists__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_pop_lists__duplicate_test') }}
    )
),
final as
(
    select * from source
)
select * from final