{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}


with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_cvs') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_jnj_consumerreach_cvs__test_date_format_odd_eve') }}
            )
)

select * from source