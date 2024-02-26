{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('myssdl_raw','sdl_my_pos_cust_mstr') }}
),
final as(
    select * from source
)

select * from final