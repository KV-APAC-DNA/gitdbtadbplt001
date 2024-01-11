{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        unique_key= ["market","prft_ctr"]
    )
}}

--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_ap_profit_center_ppm') }}
),

--Logical CTE

--Final CTE
final as (
    select
        market_code::varchar(200) as market,
        profit_centre::varchar(200) as prft_ctr,
        market_ppm_code::varchar(500) as market_portfolio,
        region_ppm_code::varchar(500) as regional_portfolio,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
)

--Final select
select * from final