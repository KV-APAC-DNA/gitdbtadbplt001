{{
    config(
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="{% if is_incremental() %}
        delete from {{this}} where file_name in (select distinct file_name from {{ source('ntasdl_raw','sdl_kr_dads_naver_gmv') }});
        {% endif %}"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_dads_naver_gmv') }} 
),
final as (
    select
        product_category_l::varchar(100) as product_category_l,
        product_category_m::varchar(100) as product_category_m,
        product_category_s::varchar(100) as product_category_s,
        product_category_detail::varchar(100) as product_category_detail,
        product_name::varchar(100) as product_name,
        product_id::varchar(100) as product_id,
        number_of_payments::varchar(100) as number_of_payments,
        quantity_of_products_paid::varchar(100) as quantity_of_products_paid,
        mobile_ratio_qty_prdt_paid::varchar(100) as mobile_ratio_qty_prdt_paid,
        payment_amount::varchar(100) as payment_amount,
        mobile_ratio_payment_amount::varchar(100) as mobile_ratio_payment_amount,
        pymt_amt_per_prdt_allowance::varchar(100) as pymt_amt_per_prdt_allowance,
        coupon_total::varchar(100) as coupon_total,
        product_coupon::varchar(100) as product_coupon,
        order_coupon::varchar(100) as order_coupon,
        refund_number::varchar(100) as refund_number,
        refund_amount::varchar(100) as refund_amount,
        refund_rate::varchar(100) as refund_rate,
        refund_qty::varchar(100) as refund_qty,
        refund_qty_paid_product::varchar(100) as refund_qty_paid_product,
        file_name::varchar(255) as file_name,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm,
        file_date::varchar(10) as file_date
    from source
)
select * from final