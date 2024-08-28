{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_perfectstore_msl') }}
    where file_name not in (
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_perfectstore_msl__null_test')}}
        union all
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_perfectstore_msl__duplicate_test')}}
        union all
        select distinct file_name from {{SOURCE('indwks_integration','TRATBL_sdl_in_perfectstore_msl__date_format_test')}})
),
final as(
    select * from source
)

select * from final