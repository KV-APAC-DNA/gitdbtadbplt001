{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('snapindsdl_raw', 'sdl_in_perfectstore_msl') }}
),
final as(
    select * from source
)

select * from final