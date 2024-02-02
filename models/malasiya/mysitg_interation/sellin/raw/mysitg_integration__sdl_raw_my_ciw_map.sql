{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('myssdl_raw','sdl_my_ciw_map') }}
),
final as(
    select * from source
)

select * from final