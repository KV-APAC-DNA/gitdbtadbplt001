with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_cdfg') }}
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