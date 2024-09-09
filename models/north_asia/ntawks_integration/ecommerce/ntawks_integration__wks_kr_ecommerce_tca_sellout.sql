with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_ecommerce_tca_sellout') }}
    where file_name not in (
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecom_naver_sellout_temp__null_test') }}
        union all
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_ecom_naver_sellout_temp__format_test') }}
    )
),
final as (
    select * from source
)
select * from final