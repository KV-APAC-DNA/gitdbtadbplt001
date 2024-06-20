{% macro build_itg_sales_store_master_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_sales_store_master_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_sales_store_master_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_sales_store_master
                {% else %}
                    {{schema}}.ntaitg_integration__itg_sales_store_master
                {% endif %}
(       channel varchar(25),
        store_type varchar(25),
        sales_grp_cd varchar(18),
        sold_to varchar(25),
        store_nm varchar(100),
        cust_store_cd varchar(18),
        ctry_cd varchar(10),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_sales_store_master
        {% else %}
            {{schema}}.ntaitg_integration__itg_sales_store_master
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}