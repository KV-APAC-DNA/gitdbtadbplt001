{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('myssdl_raw','sdl_my_as_watsons_inventory') }}
),
final as(
    select * from source
)

select * from final