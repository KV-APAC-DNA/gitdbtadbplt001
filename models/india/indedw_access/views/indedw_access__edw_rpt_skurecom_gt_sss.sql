with source as 
(
    select * from {{ ref('indedw_integration__edw_rpt_skurecom_gt_sss') }}
)
select
    mth_mm as "mth_mm",
    qtr as "qtr",
    fisc_yr as "fisc_yr",
    month as "month",
    cust_cd as "cust_cd",
    customer_name as "customer_name",
    rtruniquecode as "rtruniquecode",
    retailer_cd as "retailer_cd",
    mother_sku_cd as "mother_sku_cd",
    oos_flag as "oos_flag",
    ms_flag as "ms_flag",
    cs_flag as "cs_flag",
    soq as "soq",
    region_name as "region_name",
    zone_name as "zone_name",
    territory_name as "territory_name",
    class_desc as "class_desc",
    class_desc_a_sa_flag as "class_desc_a_sa_flag",
    channel_name as "channel_name",
    business_channel as "business_channel",
    status_desc as "status_desc",
    actv_flg as "actv_flg",
    retailer_name as "retailer_name",
    retailer_category_name as "retailer_category_name",
    csrtrcode as "csrtrcode",
    nup_target as "nup_target",
    nup_actual_ly as "nup_actual_ly",
    nup_actual_cy as "nup_actual_cy",
    achievement_nr as "achievement_nr",
    hit_ms_flag as "hit_ms_flag",
    hit_cs_flag as "hit_cs_flag",
    franchise_name as "franchise_name",
    brand_name as "brand_name",
    product_category_name as "product_category_name",
    variant_name as "variant_name",
    mothersku_name as "mothersku_name",
    salesman_code as "salesman_code",
    salesman_name as "salesman_name",
    unique_sales_code as "unique_sales_code",
    route_code as "route_code",
    route_name as "route_name",
    reco_flag as "reco_flag",
    sales_flag as "sales_flag",
    crt_dttm as "crt_dttm",
    updt_dttm as "updt_dttm"
from source