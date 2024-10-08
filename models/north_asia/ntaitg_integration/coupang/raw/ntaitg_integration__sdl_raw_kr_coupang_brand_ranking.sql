{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}
with source as (
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_brand_ranking') }}
    where file_name  not in (
        select distinct file_name from 
        {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_brand_ranking__null_test') }}
    )
),
final as (
    select * from source
)
select * from final