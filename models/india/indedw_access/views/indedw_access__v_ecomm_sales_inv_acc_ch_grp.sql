with source as 
(
    select * from {{ ref('indedw_integration__v_ecomm_sales_inv_acc_ch_grp') }}
)
select
    data_source as "data_source",
    fisc_yr as "fisc_yr",
    mth_mm as "mth_mm",
    week as "week",
    qtr as "qtr",
    month as "month",
    region_name as "region_name",
    zone_name as "zone_name",
    territory_name as "territory_name",
    customer_code as "customer_code",
    customer_name as "customer_name",
    udc_keyaccountname as "udc_keyaccountname",
    franchise_name as "franchise_name",
    brand_name as "brand_name",
    variant_name as "variant_name",
    product_category_name as "product_category_name",
    mothersku_name as "mothersku_name",
    product_name as "product_name",
    product_code as "product_code",
    channel_name_inv as "channel_name_inv",
    channel_name_sales as "channel_name_sales",
    invoice_quantity as "invoice_quantity",
    invoice_value as "invoice_value",
    bill_type as "bill_type",
    account_name as "account_name"
from source
