with 
source as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_sku_audits') }}
         where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_sku_audits__test_duplicate') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_sku_audits__null_test') }}
    )
),
final as
(
    select * from source
)
select * from final