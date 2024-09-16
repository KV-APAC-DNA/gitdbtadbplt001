with sdl_pop6_hk_promotions as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_promotions') }}
        where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_promotions__duplicate_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_promotions__null_test') }}
    )
),
final as (
    select * from sdl_pop6_hk_promotions
)
select * from final