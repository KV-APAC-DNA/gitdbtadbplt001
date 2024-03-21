with source as (
    select * from {{ source('aspsdl_raw', 'sdl_rg_travel_retail_dfs_hainan') }}
),
final as (
    select
        trim(year) as year,
        trim(mon) as mon,
        trim(yearmo) as yearmo,
        trim(retailer_name) as retailer_name,
        trim(product_department_desc) as product_department_desc,
        trim(product_department_code) as product_department_code,
        trim(brand) as brand,
        trim(product_class_desc) as product_class_desc,
        trim(product_class_code) as product_class_code,
        trim(product_subclass_desc) as product_subclass_desc,
        trim(product_subclass_code) as product_subclass_code,
        trim(brand_collection) as brand_collection,
        trim(reatiler_product_code) as reatiler_product_code,
        trim(reatiler_product_description) as reatiler_product_description,
        trim(dcl_code) as dcl_code,
        trim(ean) as ean,
        trim(style_type_code) as style_type_code,
        trim(month) as month,
        trim(door_name) as door_name,
        trim(sls_mtd_qty) as sls_mtd_qty,
        trim(sls_mtd_amt) as sls_mtd_amt,
        trim(sls_ytd_qty) as sls_ytd_qty,
        trim(sls_ytd_amt) as sls_ytd_amt,
        trim(filename) as filename,
        trim(crttd) as crttd
    from source
)
select * from final