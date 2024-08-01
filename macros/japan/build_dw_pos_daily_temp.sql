{% macro build_dw_pos_daily_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpnedw_integration.dw_pos_daily_temp
            {% else %}
                {{schema}}.jpnedw_integration__dw_pos_daily_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnedw_integration.dw_pos_daily
                {% else %}
                    {{schema}}.jpnedw_integration__dw_pos_daily
                {% endif %}
    (       		
            store_key_1 varchar(255),
            store_key_2 varchar(255),
            jan_code varchar(255),
            product_name varchar(255),
            accounting_date date,
            quantity number(18,0),
            amount number(18,0),
            account_key varchar(4),
            source_file_date varchar(30),
            upload_dt varchar(10),
            upload_time varchar(8)

    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            jpnedw_integration.dw_pos_daily
        {% else %}
            {{schema}}.jpnedw_integration__dw_pos_daily
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}