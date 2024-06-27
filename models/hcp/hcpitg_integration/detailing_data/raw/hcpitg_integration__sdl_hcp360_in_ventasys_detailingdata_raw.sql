{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('hcpsdl_raw', 'sdl_hcp360_in_ventasys_detailingdata') }}
),
final as(
    select * from source
)

select * from final