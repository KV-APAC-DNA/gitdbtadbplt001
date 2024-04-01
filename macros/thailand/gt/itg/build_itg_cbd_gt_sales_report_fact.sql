{% macro build_itg_cbd_gt_sales_report_fact(filename) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started building itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    
    {{ log("Setting query to delete records from itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set delete_from_itg_query %}
    DELETE FROM  
    {% if target.name=='prod' %}
                    thaitg_integration.itg_cbd_gt_sales_report_fact
                {% else %}
                    {{schema}}.thaitg_integration__itg_cbd_gt_sales_report_fact
                {% endif %}	
    WHERE (UPPER(bu),billing_date) IN (SELECT DISTINCT UPPER(bu),billing_date FROM {{ ref('thawks_integration__wks_cbd_gt_sales_report_fact_flag_incl') }} );
    {% endset %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Query set to delete records from itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running the query to delete records from itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(delete_from_itg_query) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running the query to delete records from itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Setting query to build itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% set build_itg_model %}
    insert into  {% if target.name=='prod' %}
                    thaitg_integration.itg_cbd_gt_sales_report_fact
                {% else %}
                    {{schema}}.thaitg_integration__itg_cbd_gt_sales_report_fact
                {% endif %}	
    with source as (
    select  * from {% if target.name=='prod' %}
                    thawks_integration.wks_cbd_gt_sales_report_fact_pre_load
                {% else %}
                    {{schema}}.thawks_integration__wks_cbd_gt_sales_report_fact_pre_load
                {% endif %}	
    ),
    final as (
        SELECT 
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
            net_sales as net_sales_usd,
            sales_rep_no,
            order_no,
            return_reason,
            payment_term,
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
    {{ log("Query setting completed to build itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Started running query to build itg table for file: "~ filename) }}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {% do run_query(build_itg_model) %}
    {{ log("-----------------------------------------------------------------------------------------------") }}
    {{ log("Completed running query to build itg table for file: "~ filename) }}
{% endmacro %}