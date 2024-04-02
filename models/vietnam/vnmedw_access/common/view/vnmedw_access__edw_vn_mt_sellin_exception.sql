with source as(
    select * from {{ ref('vnmedw_integration__edw_vn_mt_sellin_exception') }}
),
final as (
select  
    data_type AS "data_type",
    region AS "region",
    sub_channel AS "sub_channel",
    name AS "name",
    sub_category AS "sub_category",
    sales_amt_lcy AS "sales_amt_lcy",
    account AS "account",
    barcode AS "barcode",
    data_source AS "data_source",
    sales_amt_usd AS "sales_amt_usd",
    target_lcy AS "target_lcy",
    product_name AS "product_name",
    custcode AS "custcode",
    productid AS "productid",
    sub_brand AS "sub_brand",
    group_account AS "group_account",
    retail_environment AS "retail_environment",
    province AS "province",
    franchise AS "franchise",
    target_usd AS "target_usd",
    kam AS "kam",
    mnth_id AS "mnth_id",
    category AS "category"
from source
)

select * from final