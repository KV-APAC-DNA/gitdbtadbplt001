{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_perfectstore_paid_display') }}
    where file_name not in 
    (
        select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_perfectstore_paid_display__null_test') }}
        union all
        select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_perfectstore_paid_display__duplicate_test') }}
        union all
        select distinct file_name from {{ source('indwks_integration', 'TRATBL_sdl_in_perfectstore_paid_display__date_format_test') }}
    )
),
final as(
    select * from source
)

select * from final