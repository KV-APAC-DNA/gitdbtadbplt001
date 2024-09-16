with sdl_pop6_jp_products as (
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_products') }}
     where filename not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_products__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_jp_products__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_jp_products
)
select * from final