{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_la_gt_sales_order_fact') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__duplicate_test') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__test_format') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__test_format_2') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__test_format_3') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__test_format_4') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__test_format_5') }}
			union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_la_gt_sales_order_fact__test_format_6') }}
            )
),
final as (
     select 
        saleunit,
        orderid,
        orderdate,
        customer_id,
        customer_name,
        city,
        region,
        saledistrict,
        saleoffice,
        salegroup,
        customertype,
        storetype,
        saletype,
        salesemployee,
        salename,
        productid,
        productname,
        megabrand,
        brand,
        baseproduct,
        variant,
        putup,
        priceref,
        backlog,
        qty,
        subamt1,
        discount,
        subamt2,
        discountbtline,
        totalbeforevat,
        total,
        no,
        canceled,
        documentid,
        return_reason,
        promotioncode,
        promotioncode1,
        promotioncode2,
        promotioncode3,
        promotioncode4,
        promotioncode5,
        promotion_code,
        promotion_code2,
        promotion_code3,
        avgdiscount,
        ordertype,
        approverstatus,
        pricelevel,
        optional3,
        deliverydate,
        ordertime,shipto,
        billto,
        deliveryrouteid,
        approved_date,
        approved_time,
        ref_15,
        paymenttype,
        filename as file_name,
        run_id,
        crt_dttm 
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)

select * from final