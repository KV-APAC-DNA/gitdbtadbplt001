{{
    config(
        materialized="incremental",
        incremental_strategy= "append"
    )
}}

with source as(
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_cdfg') }}
    where file_name not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_cdfg__null_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_cdfg__product_lookup_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_cdfg__channel_lookup_test')}}
        )
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
        file_name AS FILE_NAME,
        current_timestamp()::timestamp_ntz(9) as CRT_DTTM 
    from source
)

select * from final