{% macro build_itg_pop6_pop_lists_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_pop6_pop_lists_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_pop6_pop_lists_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pop6_pop_lists
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pop6_pop_lists
                {% endif %}
    (           cntry_cd varchar(10),
                src_file_date varchar(10),
                status number(18,0),
                pop_list varchar(25),
                popdb_id varchar(255),
                pop_code varchar(50),
                pop_name varchar(100),
                pop_list_date date,
                hashkey varchar(200),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(2),
                file_name varchar(100),
                run_id number(14,0),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_pop6_pop_lists
        {% else %}
            {{schema}}.ntaitg_integration__itg_pop6_pop_lists
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}