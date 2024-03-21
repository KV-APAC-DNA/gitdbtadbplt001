with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_dufry_hainan') }}
),
final as (
    select
        trim(year) as year,
        trim(month) as month,
        trim(yearmo) as yearmo,
        trim(retailer_name) as retailer_name,
        trim(ean) as ean,
        trim(dcl_code) as dcl_code,
        trim(product_desc) as product_desc,
        trim(online_qty) as online_qty,
        trim(online_gmv) as online_gmv,
        trim(online_sales_split_per) as online_sales_split_per,
        trim(offline_qty) as offline_qty,
        trim(offline_gmv) as offline_gmv,
        trim(offline_sales_split_per) as offline_sales_split_per,
        trim(total_qty) as total_qty,
        trim(total_gmv) as total_gmv,
        trim(filename) as filename,
        trim(brand) as brand,
        trim(crtddt) as crtddt,
        trim(stock) as stock
    from source
)
select * from final