{% macro build_wks_la_gt_sales_order_fact_flag_incl(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build wks_la_gt_sales_order_fact_flag_incl: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_wks_la_gt_sales_order_fact_flag_incl_query %}
    create or replace table 
        {% if target.name=='prod' %} 
                thawks_integration.wks_la_gt_sales_order_fact_flag_incl
            {% else %}
                {{schema}}.thawks_integration__wks_la_gt_sales_order_fact_flag_incl
            {% endif %}	
        as (
            with wks_la_gt_sales_order_fact_pre_load as (
                select * from 
                {% if target.name=='prod' %} 
                    thawks_integration.wks_la_gt_sales_order_fact_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_sales_order_fact_pre_load
                {% endif %}	
            ),
            sdl_la_gt_sales_order_fact as (
                select *, MD5(coalesce(UPPER(saleunit),'N/A') ||coalesce(upper (orderid),'N/A') ||coalesce(to_date (orderdate , 'yyyy/mm/dd'),'9999-12-31')||coalesce(upper (productid),'N/A')
            ||coalesce(upper (customer_id),'N/A') ||coalesce(no,'N/A')) AS hashkey
            from {{ ref('thawks_integration__wks_la_gt_sales_order_fact') }} where filename= '{{filename}}'
            )
            SELECT 
                null as hashkey,
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
                wks.canceled,
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
                wks.optional3,
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
            FROM wks_la_gt_sales_order_fact_pre_load wks
            WHERE NOT EXISTS (SELECT 1
                            FROM sdl_la_gt_sales_order_fact sdl
                            WHERE wks.hashkey = sdl.hashkey)
            UNION ALL

            SELECT 
                null as hashkey,
                    sdl.saleunit,
                    sdl.orderid,
                    to_date (sdl.orderdate , 'yyyy/mm/dd') as orderdate,
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
                    cast (sdl.qty as NUMERIC(18,4)) as qty ,
                    cast (sdl.subamt1 as NUMERIC(18,4)) as subamt1,
                    cast (sdl.discount as NUMERIC(18,4)) as discount,
                    cast (sdl.subamt2 as NUMERIC(18,4)) as subamt2,
                    cast (sdl.discountbtline as NUMERIC(18,4)) as discountbtline,
                    cast (sdl.totalbeforevat as NUMERIC(18,4)) as totalbeforevat,
                    cast (sdl.total as NUMERIC(18,4)) as total,
                    sdl.no as sales_order_line_no,
                    sdl.canceled,
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
                    cast (sdl.avgdiscount as NUMERIC(18,4))  as avgdiscount,
                    sdl.ordertype,
                    sdl.approverstatus,
                    sdl.pricelevel,
                    sdl.optional3,
                    to_date(sdl.deliverydate,'yyyymmdd') as deliverydate,
                    sdl.ordertime,
                    sdl.shipto,
                    sdl.billto,
                    sdl.deliveryrouteid,
                    to_date(sdl.approved_date,'yyyymmdd') as approved_date,
                    sdl.approved_time,
                    sdl.ref_15,
                    sdl.paymenttype,
                    'I' AS load_flag,
                    sdl.filename,
                    sdl.run_id,
                    sdl.crt_dttm
            FROM sdl_la_gt_sales_order_fact sdl
            WHERE NOT EXISTS (SELECT 1
                            FROM wks_la_gt_sales_order_fact_pre_load wks
                            WHERE sdl.hashkey = wks.hashkey)
            UNION ALL
            SELECT 
                null as hashkey,
                    sdl.saleunit,
                    sdl.orderid,
                    to_date (sdl.orderdate , 'yyyy/mm/dd') as orderdate,
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
                    cast (sdl.qty as NUMERIC(18,4)) as qty ,
                    cast (sdl.subamt1 as NUMERIC(18,4)) as subamt1,
                    cast (sdl.discount as NUMERIC(18,4)) as discount,
                    cast (sdl.subamt2 as NUMERIC(18,4)) as subamt2,
                    cast (sdl.discountbtline as NUMERIC(18,4)) as discountbtline,
                    cast (sdl.totalbeforevat as NUMERIC(18,4)) as totalbeforevat,
                    cast (sdl.total as NUMERIC(18,4)) as total,
                    sdl.no as sales_order_line_no,
                    sdl.canceled,
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
                    cast (sdl.avgdiscount as NUMERIC(18,4))  as avgdiscount,
                    sdl.ordertype,
                    sdl.approverstatus,
                    sdl.pricelevel,
                    sdl.optional3,
                    to_date(sdl.deliverydate,'yyyymmdd') as deliverydate,
                    sdl.ordertime,
                    sdl.shipto,
                    sdl.billto,
                    sdl.deliveryrouteid,
                    to_date(sdl.approved_date,'yyyymmdd') as approved_date,
                    sdl.approved_time,
                    sdl.ref_15,
                    sdl.paymenttype,
                    'U' AS load_flag,
                    sdl.filename,
                    sdl.run_id,
                    sdl.crt_dttm
            FROM sdl_la_gt_sales_order_fact sdl,
            wks_la_gt_sales_order_fact_pre_load wks
                        WHERE sdl.hashkey = wks.hashkey
        );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Set the query to build wks_la_gt_sales_order_fact_flag_incl for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}

    {{ log("Started building model wks_la_gt_sales_order_fact_flag_incl for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_wks_la_gt_sales_order_fact_flag_incl_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Ended building model wks_la_gt_sales_order_fact_flag_incl for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
{% endmacro %}