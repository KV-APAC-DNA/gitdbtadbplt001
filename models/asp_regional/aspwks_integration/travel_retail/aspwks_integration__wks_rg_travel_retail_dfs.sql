with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_dfs') }}
),
final as (
    select * from source
)
select * from source