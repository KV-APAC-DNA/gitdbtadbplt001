with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_cdfg') }}
    where file_name not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_cdfg__null_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_cdfg__product_lookup_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_rg_travel_retail_cdfg__channel_lookup_test')}}
    )
),
final as (
    select
        trim(location_name) as location_name,
        trim(retailer_name) as retailer_name,
        trim(year_month) as year_month,
        trim(dcl_code) as dcl_code,
        trim(barcode) as barcode,
        trim(description) as description,
        trim(sls_qty) as sls_qty,
        trim(stock_qty) as stock_qty,
        trim(file_name) as file_name
    from source
)
select * from final