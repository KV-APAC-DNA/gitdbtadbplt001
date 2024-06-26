{% macro build_itg_sales_cust_prod_master_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_sales_cust_prod_master_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_sales_cust_prod_master_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_sales_cust_prod_master
                {% else %}
                    {{schema}}.ntaitg_integration__itg_sales_cust_prod_master
                {% endif %}
(       sales_grp_cd varchar(18),
        src_sys_cd varchar(100),
        product_nm varchar(100),
        cust_prod_cd varchar(25),
        ean_cd varchar(25),
        ctry_cd varchar(10),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_sales_cust_prod_master
        {% else %}
            {{schema}}.ntaitg_integration__itg_sales_cust_prod_master
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}