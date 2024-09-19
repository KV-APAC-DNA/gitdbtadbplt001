with 
source as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_pops') }}
     where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_pops__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_pops__duplicate_test') }}
    )
),
final as
(
    select * from source
)
select * from final