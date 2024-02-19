{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_cdfg') }}
),
final as(
    select 	
        LOCATION_NAME,
        RETAILER_NAME,
        YEAR_MONTH,
        DCL_CODE,
        BARCODE,
        DESCRIPTION,
        SLS_QTY,
        STOCK_QTY,
        FILENAME AS FILE_NAME,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM 
    from source
)

select * from final