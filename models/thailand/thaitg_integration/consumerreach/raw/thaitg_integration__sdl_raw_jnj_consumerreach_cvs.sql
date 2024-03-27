{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}


with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_consumerreach_cvs') }}
)

select * from source