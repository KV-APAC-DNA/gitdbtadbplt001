
with source as
(
    select * from dev_dna_core.thaitg_integration.itg_th_gt_sales_order
),
final as
(
    SELECT 
        cntry_cd,
        crncy_cd,
        saleunit AS distributor_id,
        orderid AS sales_order_no,
        orderdate AS sales_order_date,
        customer_id AS store_id,
        customer_name AS store_name,
        city,
        region as "region",
        saledistrict,
        saleoffice,
        salegroup,
        customertype,
        storetype,
        saletype,
        salesemployee AS sales_rep_id,
        salename AS sales_rep_name,
        productid AS product_code,
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
        ref_15 AS invoice_no,
        paymenttype,
        load_flag,
        CASE
            WHEN (
                left(
                    trim((customer_id)::text),
                    1
                ) = '_'::text
            ) THEN 'N'::text
            ELSE 'Y'::text
        END AS valid_flag
    FROM source
    WHERE 
    (
        (
            (
                upper((load_flag)::text) <> 'D'::text
            )
            AND (
                (cancelled)::text <> (1)::text
            )
        )
        AND (
            upper((approverstatus)::text) = 'Y'::text
        )
    )
)
select * from final