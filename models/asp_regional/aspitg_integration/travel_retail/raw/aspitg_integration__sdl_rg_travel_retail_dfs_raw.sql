{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_dfs') }}
),
final as(
    select *,
        current_timestamp() as crt_dttm 
    from source
)

select * from final