with sdl_pop6_jp_product_lists_pops as 
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_lists_pops') }}
     where file_name not in (
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_exclusion__lookup_test_1') }}
        union all
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_exclusion__lookup_test_2') }}
    )
),
final as (
    select * from sdl_pop6_jp_product_lists_pops
)
select * from final