{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['transaction_date'],
        pre_hook= "delete from {{this}} where transaction_date >= (select min(time_period) from DEV_DNA_LOAD.SNAPPCFSDL_RAW.SDL_COMPETITIVE_BANNER_GROUP);"
    )
}}

with source as(
    select * from DEV_DNA_LOAD.SNAPPCFSDL_RAW.SDL_COMPETITIVE_BANNER_GROUP
),
final as(
    select 
        market::varchar(2000) as market,
        banner::varchar(2000) as banner,
        banner_classification::varchar(255) as banner_classification,
        manufacturer::varchar(255) as manufacturer,
        brand::varchar(255) as brand,
        sku_name::varchar(255) as sku_name,
        apn::varchar(20) AS ean_number,
        time_period::timestamp_ntz(9) AS transaction_date,
        unit::float as unit,
        dollar::float as dollar,
        'Pacific'::varchar(10) AS country,
        'AUD'::varchar(10) AS currency ,
        current_timestamp()::timestamp_ntz(9) AS crt_dttm,
        NULL::varchar(250) AS source_file_name
    from source 
)
select * from final