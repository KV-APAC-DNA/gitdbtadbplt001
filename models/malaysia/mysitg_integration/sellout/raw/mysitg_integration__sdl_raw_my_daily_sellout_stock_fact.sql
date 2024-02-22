{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ ref('mysitg_integration__sdl_my_daily_sellout_stock_fact') }}
),
final as (
    select * from source
)

select * from final