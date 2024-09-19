with 
source as
(
    select * from {{ source('jpnsdl_raw', 'sdl_pop6_jp_product_attribute_audits') }}
     where file_name not in (
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_product_attribute_audits__null_test') }}
        union all
        select distinct file_name from {{ source('jpnwks_integration', 'TRATBL_sdl_pop6_jp_product_attribute_audits__test_duplicate') }}
    ) 
),
final as
(
    select * from source
)
select * from final