{{
    config
    (
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with 
source as
(
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_daily_brand_reviews') }} 
    where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_daily_brand_reviews__null_test') }}
      union all
      select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_daily_brand_reviews__test_date_format_odd_eve_leap') }}
     )
),
final as
(
    select
        *
    from source
)
select * from final