with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_pop6_hk_product_attribute_audits') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_product_attribute_audits__duplicate_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_pop6_hk_product_attribute_audits__null_test') }}
    )
),
final as
(
    select * from source
)
select * from final