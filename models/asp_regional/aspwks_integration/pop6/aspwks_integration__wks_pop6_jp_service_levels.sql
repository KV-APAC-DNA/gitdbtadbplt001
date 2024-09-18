with sdl_pop6_jp_service_levels as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_service_levels') }}
     where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_exclusion__lookup_test_1') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_exclusion__lookup_test_2') }}
    )
),
final as (
    select * from sdl_pop6_jp_service_levels
)
select * from final