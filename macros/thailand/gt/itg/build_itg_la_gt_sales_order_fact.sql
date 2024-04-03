{% macro build_itg_la_gt_sales_order_fact(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    DELETE FROM 
    {% if target.name=='prod' %}
                    thaitg_integration.itg_la_gt_sales_order_fact
                {% else %}
                    {{schema}}.thaitg_integration__itg_la_gt_sales_order_fact
                {% endif %}	
    WHERE (UPPER(saleunit),orderdate) IN (
        SELECT DISTINCT UPPER(saleunit),orderdate FROM 
        {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_sales_order_fact_flag_incl
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_sales_order_fact_flag_incl
                {% endif %}	
        );
    {% endset %}
    { log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
    insert into  {% if target.name=='prod' %}
                    thaitg_integration.itg_la_gt_sales_order_fact
                {% else %}
                    {{schema}}.thaitg_integration__itg_la_gt_sales_order_fact
                {% endif %}	
        with source as (
        select  * from 
                {% if target.name=='prod' %}
                    thawks_integration.wks_la_gt_sales_order_fact_flag_incl
                {% else %}
                    {{schema}}.thawks_integration__wks_la_gt_sales_order_fact_flag_incl
                {% endif %}
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
                sales_order_line_no,
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
                ordertime,
                shipto,
                billto,
                deliveryrouteid,
                approved_date,
                approved_time,
                ref_15,
                paymenttype,
                load_flag,
                filename,
                run_id,
                crt_dttm,
                current_timestamp()::timestamp_ntz(9) as updt_dttm
        from source
        )

        select * from final;
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query setting completed to build itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table -> itg_la_gt_sales_order_fact for file: "~ filename) }}
{% endmacro %}