with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_cnsc') }}
),
final as (
    select * from source
)
select * from source