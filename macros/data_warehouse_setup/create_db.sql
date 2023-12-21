{% macro create_db(db_name) %}
    {% set create_query %}
        create database {{ db_name }};
    {% endset %}

    {% do run_query(create_query) %}
{% endmacro %}