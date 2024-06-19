{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('snapindsdl_raw', 'sdl_in_perfectstore_sos') }}
),
final as(
    select * from source
)

select * from final