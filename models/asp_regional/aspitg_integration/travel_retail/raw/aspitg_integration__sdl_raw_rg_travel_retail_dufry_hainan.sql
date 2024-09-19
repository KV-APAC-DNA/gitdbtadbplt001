{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_dufry_hainan') }}
    where filename not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_dufry_hainan__null_test')}}
    )
),
final as(
    select * from source
)

select * from final