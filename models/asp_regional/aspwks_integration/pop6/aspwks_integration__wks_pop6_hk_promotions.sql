with sdl_pop6_hk_promotions as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_promotions') }}
        where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_exclusion__lookup_test_1') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_exclusion__lookup_test_2') }}
    )
),
final as (
    select * from sdl_pop6_hk_promotions
)
select * from final