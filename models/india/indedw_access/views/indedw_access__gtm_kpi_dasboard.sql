with source as 
(
    select * from {{ source('indedw_integration', 'gtm_kpi_dasboard') }}
)
select
    fisc_per as "fisc_per",
    week as "week",
    data_type as "data_type",
    region_name as "region_name",
    zone_name as "zone_name",
    territory_name as "territory_name",
    channel_name as "channel_name",
    customer_code as "customer_code",
    customer_name as "customer_name",
    retailer_category_name as "retailer_category_name",
    retailer_class as "retailer_class",
    urc as "urc",
    retailer_channel_2 as "retailer_channel_2",
    num_buying_retailers as "num_buying_retailers",
    unique_sales_code as "unique_sales_code",
    salesman_name as "salesman_name",
    franchise_name as "franchise_name",
    brand_name as "brand_name",
    variant_name as "variant_name",
    mothersku_name as "mothersku_name",
    gtm_flag as "gtm_flag",
    nr_value as "nr_value",
    cust_control_nr_value as "cust_control_nr_value",
    num_bills as "num_bills",
    retailer_code as "retailer_code",
    gtm_year as "gtm_year",
    gtm_month as "gtm_month",
    pre_post_flag as "pre_post_flag",
    customer_gtm_flag as "customer_gtm_flag",
    retailer_gtm_flag as "retailer_gtm_flag",
    retailer_tag as "retailer_tag",
    pack_status as "pack_status"
from source
