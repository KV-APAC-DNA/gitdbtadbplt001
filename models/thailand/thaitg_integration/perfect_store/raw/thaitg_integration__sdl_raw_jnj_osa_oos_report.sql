{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}


with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_osa_oos_report') }}
)

select * from source