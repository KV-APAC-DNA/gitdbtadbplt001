with sdl_pop6_tw_product_lists_pops as 
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_tw_product_lists_pops') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_tw_product_lists_pops__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_tw_product_lists_pops__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_tw_product_lists_pops
)
select * from final