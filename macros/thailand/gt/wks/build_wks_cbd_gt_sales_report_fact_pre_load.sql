{% macro build_wks_cbd_gt_sales_report_fact_pre_load(filename) %}
    {% set tablename %}
    {% if target.name=='prod' %}
                    thawks_integration.wks_cbd_gt_sales_report_fact_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_cbd_gt_sales_report_fact_pre_load
                {% endif %}	
    {% endset %}
    {% set query %}
    CREATE TABLE if not exists {{tablename}}
    (
        hashkey varchar(500),
        bu varchar(50),
        client varchar(50),
        sub_client varchar(50),
        product_code varchar(50),
        product_name varchar(200),
        billing_no varchar(50),
        billing_date date,
        batch_no varchar(50),
        expiry_date date,
        customer_code varchar(50),
        customer_name varchar(100),
        distribution_channel varchar(50),
        customer_group varchar(100),
        province varchar(50),
        sales_qty numeric(18,4),
        foc_qty numeric(18,4),
        net_price numeric(18,4),
        net_sales numeric(18,4),
        sales_rep_no varchar(50),
        order_no varchar(50),
        return_reason varchar(200),
        payment_term varchar(50),
        filename varchar(50),
        run_id varchar(14),
        crt_dttm timestamp without time zone
    );
    TRUNCATE TABLE {{tablename}};
    Insert into {{tablename}}
        (
        hashkey ,
        bu,
        client,
        sub_client,
        product_code,
        product_name,
        billing_no,
        billing_date,
        batch_no,
        expiry_date,
        customer_code,
        customer_name,
        distribution_channel,
        customer_group,
        province,
        sales_qty,
        foc_qty,
        net_price,
        net_sales,
        sales_rep_no,
        order_no,
        return_reason,
        payment_term,
        filename,
        run_id,
        crt_dttm
        )
        SELECT MD5(coalesce(UPPER(product_code),'N/A') ||coalesce(billing_date ,'9999-12-31')||coalesce(upper (batch_no),'N/A') ||coalesce(upper (customer_code),'N/A')
        ||coalesce(upper (sales_rep_no),'N/A') ||coalesce(order_no,'N/A')) AS hashkey,
        bu,
        client,
        sub_client,
        product_code,
        product_name,
        billing_no,
        billing_date,
        batch_no,
        expiry_date,
        customer_code,
        customer_name,
        distribution_channel,
        customer_group,
        province,
        sales_qty,
        foc_qty,
        net_price,
        net_sales_usd,
        sales_rep_no,
        order_no,
        return_reason,
        payment_term,
        filename,
        run_id,
        crt_dttm
        FROM
        {% if target.name=='prod' %}
                    thaitg_integration.itg_cbd_gt_sales_report_fact
                {% else %}
                    {{schema}}.thaitg_integration__itg_cbd_gt_sales_report_fact
                {% endif %}
        WHERE billing_date >= (SELECT MIN(billing_date) FROM {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }} where filename = '{{filename}}'
        )
        AND   UPPER(bu) IN (SELECT DISTINCT UPPER(bu)
                                FROM {{ source('thasdl_raw', 'sdl_cbd_gt_sales_report_fact') }}
                                where filename = '{{filename}}'
                                );
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}