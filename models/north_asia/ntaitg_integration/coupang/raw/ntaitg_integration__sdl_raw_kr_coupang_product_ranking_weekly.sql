{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_product_ranking_weekly') }}
)
select * from source