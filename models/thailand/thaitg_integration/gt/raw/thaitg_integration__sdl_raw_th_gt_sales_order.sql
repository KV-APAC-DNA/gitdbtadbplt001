{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )
}}

with source as (
    select * from {{ source('thasdl_raw', 'sdl_th_gt_sales_order') }}
    where file_name not in (
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__null_test') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__duplicate_test') }}
            union all 
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__test_format') }}
            union all
            select distinct file_name from {{ source('thawks_integration', 'TRATBL_sdl_th_gt_sales_order__test_date_format_odd_eve_leap') }}
            )
),

final as (
    SELECT 
        hashkey,
        cntry_cd,
        crncy_cd,
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
        sales_order_line_no,
        cancelled,
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
        optional,
        deliverydate,
        ordertime,
        shipto,
        billto,
        deliveryrouteid,
        approved_date,
        approved_time,
        ref_15,
        paymenttype,
        filename,
        run_id,
        crt_dttm
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final