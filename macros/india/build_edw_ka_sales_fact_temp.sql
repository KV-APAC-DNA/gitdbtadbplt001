{% macro build_edw_ka_sales_fact_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                indedw_integration.edw_ka_sales_fact_temp
            {% else %}
                {{schema}}.indedw_integration__edw_ka_sales_fact_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    indedw_integration.edw_ka_sales_fact
                {% else %}
                    {{schema}}.indedw_integration__edw_ka_sales_fact
                {% endif %}
    (           	customer_code varchar(50),
                    invoice_date number(18,0),
                    retailer_code varchar(50),
                    retailer_name varchar(200),
                    product_code varchar(50),
                    invoice_no varchar(100),
                    prdqty number(18,0),
                    prdtaxamt number(38,6),
                    prdschdiscamt number(38,6),
                    prddbdiscamt number(38,6),
                    salwdsamt number(38,6),
                    saleflag varchar(10),
                    confirmsales varchar(10),
                    subtotal4 number(21,3),
                    totalgrosssalesincltax number(38,6),
                    totalsalesnr number(38,6),
                    totalsalesconfirmed number(38,6),
                    totalsalesnrconfirmed number(38,6),
                    totalsalesunconfirmed number(38,6),
                    totalsalesnrunconfirmed number(38,6),
                    totalqtyconfirmed number(18,0),
                    totalqtyunconfirmed number(18,0),
                    buyingoutlets varchar(100),
                    crt_dttm timestamp_ntz(9),
                    updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            indedw_integration.edw_ka_sales_fact
        {% else %}
            {{schema}}.indedw_integration__edw_ka_sales_fact
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}