{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('ntasdl_raw', 'sdl_kr_coupang_product_master') }}
    where file_name not in (
        select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_coupang_product_master__null_test') }}
    )
)
select * from source