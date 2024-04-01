{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}



with source as(
    select * from {{ source('thasdl_raw', 'sdl_jnj_mer_share_of_shelf') }}
)

select * from source