{% macro build_wks_th_gt_sales_order_flag_incl(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build wks_th_gt_sales_order_flag_incl: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_wks_th_gt_sales_order_flag_incl_query %}
    create or replace table 
        {% if target.name=='prod' %} 
                thawks_integration.wks_th_gt_sales_order_flag_incl
            {% else %}
                {{schema}}.thawks_integration__wks_th_gt_sales_order_flag_incl
            {% endif %}	
        as (
            with wks_th_gt_sales_order_pre_load as (
            select * from
                {% if target.name=='prod' %} 
                    thawks_integration.wks_th_gt_sales_order_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_th_gt_sales_order_pre_load
                {% endif %}	
        ),
        sdl_th_gt_sales_order as (
            select * from {{ source('thasdl_raw', 'sdl_th_gt_sales_order') }} where filename= '{{filename}}'
        )

        SELECT 
            wks.cntry_cd,
            wks.crncy_cd,
            wks.saleunit,
            wks.orderid,
            wks.orderdate,
            wks.customer_id,
            wks.customer_name,
            wks.city,
            wks.region,
            wks.saledistrict,
            wks.saleoffice,
            wks.salegroup,
            wks.customertype,
            wks.storetype,
            wks.saletype,
            wks.salesemployee,
            wks.salename,
            wks.productid,
            wks.productname,
            wks.megabrand,
            wks.brand,
            wks.baseproduct,
            wks.variant,
            wks.putup,
            wks.priceref,
            wks.backlog,
            wks.qty,
            wks.subamt1,
            wks.discount,
            wks.subamt2,
            wks.discountbtline,
            wks.totalbeforevat,
            wks.total,
            wks.sales_order_line_no,
            wks.cancelled,
            wks.documentid,
            wks.return_reason,
            wks.promotioncode,
            wks.promotioncode1,
            wks.promotioncode2,
            wks.promotioncode3,
            wks.promotioncode4,
            wks.promotioncode5,
            wks.promotion_code,
            wks.promotion_code2,
            wks.promotion_code3,
            wks.avgdiscount,
            wks.ordertype,
            wks.approverstatus,
            wks.pricelevel,
            wks.optional,
            wks.deliverydate,
            wks.ordertime,
            wks.shipto,
            wks.billto,
            wks.deliveryrouteid,
            wks.approved_date,
            wks.approved_time,
            wks.ref_15,
            wks.paymenttype,
            'D' AS load_flag,
            wks.filename,
            wks.run_id,
            wks.crt_dttm
        FROM wks_th_gt_sales_order_pre_load wks
        WHERE NOT EXISTS (SELECT 1
                        FROM sdl_th_gt_sales_order sdl
                        WHERE wks.hashkey = sdl.hashkey)

        UNION ALL

        SELECT 
            sdl.cntry_cd,
            sdl.crncy_cd,
            sdl.saleunit,
            sdl.orderid,
            sdl.orderdate,
            sdl.customer_id,
            sdl.customer_name,
            sdl.city,
            sdl.region,
            sdl.saledistrict,
            sdl.saleoffice,
            sdl.salegroup,
            sdl.customertype,
            sdl.storetype,
            sdl.saletype,
            sdl.salesemployee,
            sdl.salename,
            sdl.productid,
            sdl.productname,
            sdl.megabrand,
            sdl.brand,
            sdl.baseproduct,
            sdl.variant,
            sdl.putup,
            sdl.priceref,
            sdl.backlog,
            sdl.qty,
            sdl.subamt1,
            sdl.discount,
            sdl.subamt2,
            sdl.discountbtline,
            sdl.totalbeforevat,
            sdl.total,
            sdl.sales_order_line_no,
            sdl.cancelled,
            sdl.documentid,
            sdl.return_reason,
            sdl.promotioncode,
            sdl.promotioncode1,
            sdl.promotioncode2,
            sdl.promotioncode3,
            sdl.promotioncode4,
            sdl.promotioncode5,
            sdl.promotion_code,
            sdl.promotion_code2,
            sdl.promotion_code3,
            sdl.avgdiscount,
            sdl.ordertype,
            sdl.approverstatus,
            sdl.pricelevel,
            sdl.optional,
            sdl.deliverydate,
            sdl.ordertime,
            sdl.shipto,
            sdl.billto,
            sdl.deliveryrouteid,
            sdl.approved_date,
            sdl.approved_time,
            sdl.ref_15,
            sdl.paymenttype,
            'I' AS load_flag,
            sdl.filename,
            sdl.run_id,
            sdl.crt_dttm
        FROM sdl_th_gt_sales_order sdl
        WHERE NOT EXISTS (SELECT 1
                        FROM wks_th_gt_sales_order_pre_load wks
                        WHERE sdl.hashkey = wks.hashkey)

        UNION ALL

        SELECT 
            sdl.cntry_cd,
            sdl.crncy_cd,
            sdl.saleunit,
            sdl.orderid,
            sdl.orderdate,
            sdl.customer_id,
            sdl.customer_name,
            sdl.city,
            sdl.region,
            sdl.saledistrict,
            sdl.saleoffice,
            sdl.salegroup,
            sdl.customertype,
            sdl.storetype,
            sdl.saletype,
            sdl.salesemployee,
            sdl.salename,
            sdl.productid,
            sdl.productname,
            sdl.megabrand,
            sdl.brand,
            sdl.baseproduct,
            sdl.variant,
            sdl.putup,
            sdl.priceref,
            sdl.backlog,
            sdl.qty,
            sdl.subamt1,
            sdl.discount,
            sdl.subamt2,
            sdl.discountbtline,
            sdl.totalbeforevat,
            sdl.total,
            sdl.sales_order_line_no,
            sdl.cancelled,
            sdl.documentid,
            sdl.return_reason,
            sdl.promotioncode,
            sdl.promotioncode1,
            sdl.promotioncode2,
            sdl.promotioncode3,
            sdl.promotioncode4,
            sdl.promotioncode5,
            sdl.promotion_code,
            sdl.promotion_code2,
            sdl.promotion_code3,
            sdl.avgdiscount,
            sdl.ordertype,
            sdl.approverstatus,
            sdl.pricelevel,
            sdl.optional,
            sdl.deliverydate,
            sdl.ordertime,
            sdl.shipto,
            sdl.billto,
            sdl.deliveryrouteid,
            sdl.approved_date,
            sdl.approved_time,
            sdl.ref_15,
            sdl.paymenttype,
            'U' AS load_flag,
            sdl.filename,
            sdl.run_id,
            sdl.crt_dttm
        FROM sdl_th_gt_sales_order sdl,
        wks_th_gt_sales_order_pre_load wks
              WHERE sdl.hashkey = wks.hashkey    
        );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Set the query to build wks_th_gt_sales_order_flag_incl for file: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {{ log("Started building model wks_th_gt_sales_order_flag_incl for file: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_th_gt_sales_order_flag_incl_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Ended building model wks_th_gt_sales_order_flag_incl for file: "~ file) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}