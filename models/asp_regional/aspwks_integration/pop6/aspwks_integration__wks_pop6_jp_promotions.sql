with sdl_pop6_jp_promotions as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_promotions') }}
     where file_name not in (
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_promotions__null_test') }}
        union all
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_promotions__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_jp_promotions
)
select * from final