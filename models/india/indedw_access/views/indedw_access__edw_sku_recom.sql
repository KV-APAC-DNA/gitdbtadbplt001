with source as 
(
    select * from {{ source('indedw_integration', 'edw_sku_recom') }}
)
select
    cust_cd as "cust_cd",
    retailer_cd as "retailer_cd",
    product_code as "product_code",
    mother_sku_cd as "mother_sku_cd",
    salesman_code as "salesman_code",
    route_code as "route_code",
    rtruniquecode as "rtruniquecode",
    customer_name as "customer_name",
    region_name as "region_name",
    zone_name as "zone_name",
    territory_name as "territory_name",
    class_desc as "class_desc",
    channel_name as "channel_name",
    business_channel as "business_channel",
    status_desc as "status_desc",
    product_name as "product_name",
    franchise_name as "franchise_name",
    brand_name as "brand_name",
    product_category_name as "product_category_name",
    variant_name as "variant_name",
    mothersku_name as "mothersku_name",
    mth_mm as "mth_mm",
    qtr as "qtr",
    fisc_yr as "fisc_yr",
    month as "month",
    retailer_name as "retailer_name",
    salesman_name as "salesman_name",
    unique_sales_code as "unique_sales_code",
    route_name as "route_name",
    quantity as "quantity",
    achievement_nr_val as "achievement_nr_val",
    achievement_amt as "achievement_amt",
    num_packs as "num_packs",
    num_lines as "num_lines",
    retailer_category_name as "retailer_category_name",
    csrtrcode as "csrtrcode",
    oos_flag as "oos_flag",
    ms_flag as "ms_flag",
    cs_flag as "cs_flag",
    soq as "soq",
    hit_oos_flag as "hit_oos_flag",
    hit_ms_flag as "hit_ms_flag",
    hit_cs_flag as "hit_cs_flag",
    target_ms_mothersku_name as "target_ms_mothersku_name",
    target_cs_mothersku_name as "target_cs_mothersku_name",
    orange_percentage as "orange_percentage"
from source
