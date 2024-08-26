with sdl_pop6_th_product_lists_products as 
(
    select * from {{ source('thasdl_raw', 'sdl_pop6_th_product_lists_products') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_product_lists_products__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_pop6_th_product_lists_products__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_th_product_lists_products
)
select * from final