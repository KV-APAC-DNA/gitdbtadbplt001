with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_kr_exclusion') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_exclusion__lookup_test_1') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_kr_exclusion__lookup_test_2') }}
    )
),
final as
(
    select * from source
)
select * from final