{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_search_keyword_by_category') }}
),
final as (
    select * from source
)
select * from final