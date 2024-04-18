with source as (
    select * from {{ ref('vnmedw_integration__edw_vn_mt_offtake_exception_report') }}
),
final as (
select  
    sub_brand AS "sub_brand",
    month_id AS "month_id",
    category AS "category",
    barcode AS "barcode",
    amount_usd AS "amount_usd",
    amount AS "amount",
    customer_cd AS "customer_cd",
    franchise AS "franchise",
    product_cd AS "product_cd",
    store_name AS "store_name",
    product_name AS "product_name",
    account AS "account",
    sub_category AS "sub_category"
from source
)

select * from final