with sdl_pop6_sg_product_lists_pops as 
(
    select * from {{ source('sgpsdl_raw', 'sdl_pop6_sg_product_lists_pops') }}
    where file_name not in (
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_kr_users__null_test') }}
        union all
        select distinct file_name from {{ source('sgpwks_integration', 'TRATBL_sdl_pop6_kr_users__duplicate_test') }}
    )
),
final as (
    select * from sdl_pop6_sg_product_lists_pops
)
select * from final