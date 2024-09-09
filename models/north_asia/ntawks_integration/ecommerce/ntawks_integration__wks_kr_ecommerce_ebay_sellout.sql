with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_ebay_sellout') }}
    where file_name not in (
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecommerce_ebay_sellout__null_test') }}
        union all
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecommerce_ebay_sellout__lookup_test') }}
    )
),
final as (
    select * from source
)
select * from final