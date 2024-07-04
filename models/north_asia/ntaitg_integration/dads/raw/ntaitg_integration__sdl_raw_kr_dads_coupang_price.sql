{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_coupang_price') }} 
),
final as (
    select 
    report_date
,trusted_upc
,trusted_rpc
,trusted_mpc
,trusted_product_description
,region
,online_store
,brand
,manufacturer
,category
,dimension1
,Sub_Category
,Brand_SubCategory
,dimension4
,dimension5
,dimension6
,Seller
,Power_SKU
,availability_status
,currency
,observed_price
,store_list_price
,min_price
,max_price
,min_max_diff_pct
,min_max_diff_price
,msrp
,msrp_diff_pct
,msrp_diff_amount
,previous_day_price
,previous_day_diff_pct
,previous_day_diff_amount
,promotion_text
,url,  
null as file_name,
current_timestamp() as crtd_dttm
from source
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
{% endif %}
)
select * from final