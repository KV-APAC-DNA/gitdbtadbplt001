{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_customer_brand_trend') }} where filename  not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_customer_brand_trend__null_test') }}
        union all
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_customer_brand_trend__test_date_format_odd_eve_leap') }}
    )
),
final as (
    select * from source
)
select * from final