{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_promotion') }}
),
final as(
    select * from source
)

select * from final