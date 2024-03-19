with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_dfs') }}
),
final as (
    select
        trim(retailer_name) as retailer_name,
        trim(year_month) as year_month,
        trim(product_department) as product_department,
        trim(brand) as brand,
        trim(product_class) as product_class,
        trim(common_sku) as common_sku,
        trim(common_sku_status) as common_sku_status,
        trim(common_sku_type) as common_sku_type,
        trim(style) as style,
        trim(region_sku) as region_sku,
        trim(vendor_style) as vendor_style,
        trim(metrics) as metrics,
        trim(location_name) as location_name,
        trim(sls_qty) as sls_qty,
        trim(sls_amt) as sls_amt,
        trim(soh_qty) as soh_qty,
        trim(file_name) as file_name
    from source
)
select * from final