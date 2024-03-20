with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_sales_stock') }}
),
final as (
    select
        trim(location_name) as location_name,
        trim(retailer_name) as retailer_name,
        trim(year) as year,
        trim(month) as month,
        trim(dcl_code) as dcl_code,
        trim(sap_code) as sap_code,
        trim(reference) as reference,
        trim(product_desc) as product_desc,
        trim(size) as size,
        trim(rsp) as rsp,
        trim(c_sls_qty) as c_sls_qty,
        trim(c_sls_amt) as c_sls_amt,
        trim(c_stock_qty) as c_stock_qty,
        trim(c_stock_amt) as c_stock_amt,
        trim(buffer) as buffer,
        trim(mix) as mix,
        trim(r_3m) as r_3m,
        trim(comparison) as comparison,
        trim(sls_qty) as sls_qty,
        trim(stock_qty) as stock_qty,
        trim(file_name) as file_name
    from source
)
select * from final