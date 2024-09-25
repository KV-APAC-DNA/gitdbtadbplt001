with sdl_pop6_jp_promotion_plans as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_promotion_plans') }}
     where file_name not in (
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_promotion_plans__null_test') }}
        union all
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_promotion_plans__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_jp_promotion_plans
)
select * from final