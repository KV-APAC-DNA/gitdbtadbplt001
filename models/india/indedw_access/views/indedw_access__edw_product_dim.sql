with source as 
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
)
select
    product_code as "product_code",
    product_name as "product_name",
    product_desc as "product_desc",
    franchise_name as "franchise_name",
    brand_name as "brand_name",
    product_category_name as "product_category_name",
    variant_name as "variant_name",
    mothersku_name as "mothersku_name",
    uom as "uom",
    std_nr as "std_nr",
    case_lot as "case_lot",
    sale_uom as "sale_uom",
    sale_conversion_factor as "sale_conversion_factor",
    base_uom as "base_uom",
    int_uom as "int_uom",
    gross_wt as "gross_wt",
    net_wt as "net_wt",
    active_flag as "active_flag",
    delete_flag as "delete_flag",
    shelf_life as "shelf_life",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm",
    franchise_code as "franchise_code",
    brand_code as "brand_code",
    product_category_code as "product_category_code",
    variant_code as "variant_code",
    mothersku_code as "mothersku_code"
from source
