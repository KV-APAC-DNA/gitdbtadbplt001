{{
    config(
        materialized='incremental',
        incremental_strategy='append'
    )
}}

with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_ecommerce_6pai') }}
),
final as 
(
    select * from source
)
select * from final