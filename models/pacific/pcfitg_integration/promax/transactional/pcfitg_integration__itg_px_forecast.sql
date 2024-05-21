{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=["est_date"],
        pre_hook= "delete from {{this}} where est_date >= DATEADD(MONTH, -42, convert_timezone('UTC', current_timestamp())::timestamp_ntz(9))"
    )
}}
with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_px_forecast') }}
),
final as
(  
    select
    ac_code::varchar(10) as ac_code,
    ac_attribute::varchar(20) as ac_attribute,
    sku_stockcode::varchar(18)as sku_stockcode,
    sku_attribute::varchar(20) as sku_attribute,
    sku_profitcentre::varchar(10) as sku_profitcentre,
    est_date::date as est_date,
    est_normal::number(18,0) as est_normal,
    est_promotional::number(18,0) as est_promotional,
    est_estimate::number(18,0) as est_estimate
    from source
)
select * from final