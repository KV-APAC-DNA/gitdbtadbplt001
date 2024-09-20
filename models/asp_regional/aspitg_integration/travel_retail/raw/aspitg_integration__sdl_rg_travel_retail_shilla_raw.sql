{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_shilla') }}
    where file_name not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_shilla__null_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_shilla__product_lookup_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_shilla__channel_lookup_test')}}
    )
),
final as(
    select *,
       current_timestamp()::timestamp_ntz(9) as crt_dttm 
    from source
)

select * from final