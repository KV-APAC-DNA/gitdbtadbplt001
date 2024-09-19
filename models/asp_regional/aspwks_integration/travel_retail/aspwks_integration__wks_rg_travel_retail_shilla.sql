with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_shilla') }}
),
final as (
    select
        trim(retailer_name) as retailer_name,
        trim(year_month) as year_month,
        trim(brand) as brand,
        trim(sku) as sku,
        trim(description) as description,
        trim(dcl_code) as dcl_code,
        trim(ean) as ean,
        trim(color) as color,
        trim(location_name) as location_name,
        trim(sls_qty) as sls_qty,
        trim(sls_amt) as sls_amt,
        trim(file_name) as file_name
    from source
)
select * from final