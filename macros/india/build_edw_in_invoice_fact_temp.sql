{% macro build_edw_in_invoice_fact_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                indedw_integration.edw_in_invoice_fact_temp
            {% else %}
                {{schema}}.indedw_integration__edw_in_invoice_fact_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    indedw_integration.edw_in_invoice_fact
                {% else %}
                    {{schema}}.indedw_integration__edw_in_invoice_fact
                {% endif %}
    (           	customer_code varchar(10),
                    product_code varchar(18),
                    invoice_no varchar(10),
                    invoice_date date,
                    invoice_val number(38,17),
                    invoice_qty float,
                    wt_invoice_qty float,
                    crt_dttm timestamp_ntz(9),
                    updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            indedw_integration.edw_in_invoice_fact
        {% else %}
            {{schema}}.indedw_integration__edw_in_invoice_fact
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}