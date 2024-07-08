{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('myssdl_raw', 'sdl_my_perfectstore_outlet_mst') }}
)
select * from source