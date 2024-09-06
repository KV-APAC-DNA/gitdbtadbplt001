with source as 
(
    select * from {{ ref('indedw_integration__v_rpt_fran_chnl_actl') }}
)
select
    fisc_yr as "fisc_yr",
    fisc_mnth as "fisc_mnth",
    fisc_wk as "fisc_wk",
    region_name as "region_name",
    zone_name as "zone_name",
    territory_name as "territory_name",
    channel as "channel",
    retailer_channel_level1 as "retailer_channel_level1",
    retailer_channel_level2 as "retailer_channel_level2",
    retailer_channel_level3 as "retailer_channel_level3",
    customer_code as "customer_code",
    customer_name as "customer_name",
    product_category1 as "product_category1",
    product_category2 as "product_category2",
    product_category3 as "product_category3",
    product_category4 as "product_category4",
    franchise_name as "franchise_name",
    brand_name as "brand_name",
    variant_name as "variant_name",
    mothersku_name as "mothersku_name",
    datasource as "datasource",
    sls_actl_val as "sls_actl_val"
from source
