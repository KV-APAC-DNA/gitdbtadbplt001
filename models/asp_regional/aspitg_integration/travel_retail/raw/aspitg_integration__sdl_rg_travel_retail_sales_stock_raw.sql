{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_sales_stock') }}
),
final as(
    select * from source
)

select * from final