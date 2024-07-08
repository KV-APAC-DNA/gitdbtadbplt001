{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('indsdl_raw', 'sdl_in_perfectstore_paid_display') }}
),
final as(
    select * from source
)

select * from final