{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('sgpsdl_raw','sdl_sg_scan_data_amazon') }}
),
final as(
    select * from source
)

select * from final