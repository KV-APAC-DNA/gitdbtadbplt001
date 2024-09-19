with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_cnsc') }}
),
final as (
    select
        trim(door_name) as door_name,
        trim(yearmo) as yearmo,
        trim(retailer_name) as retailer_name,
        trim(supplier_product_code) as supplier_product_code,
        trim(product_description) as product_description,
        trim(brand) as brand,
        trim(pack_size) as pack_size,
        trim(serial_number) as serial_number,
        trim(inventory_qty) as inventory_qty,
        trim(sales_qty) as sales_qty,
        trim(sales_amount) as sales_amount,
        trim(material_code) as material_code,
        trim(ean) as ean,
        trim(dcl_code) as dcl_code,
        trim(filename) as filename,
        trim(store_sales) as store_sales,
        trim(total_store_sales) as total_store_sales,
        trim(no_of_ecommerce_sales) as no_of_ecommerce_sales,
        trim(total_ecommerce_sales) as total_ecommerce_sales,
        trim(membership_sls_qty) as membership_sls_qty,
        trim(membership_sls_amt) as membership_sls_amt,
        trim(crttd) as crttd
    from source
)
select * from final