{% macro build_itg_pop6_pops_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_pop6_pops_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_pop6_pops_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pop6_pops
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pop6_pops
                {% endif %}
    (       	cntry_cd varchar(10),
                src_file_date varchar(10),
                status number(18,0),
                popdb_id varchar(255),
                pop_code varchar(50),
                pop_name varchar(100),
                address varchar(255),
                longitude number(18,5),
                latitude number(18,5),
                country varchar(200),
                channel varchar(200),
                retail_environment_ps varchar(200),
                customer varchar(200),
                sales_group_code varchar(200),
                sales_group_name varchar(200),
                customer_grade varchar(200),
                external_pop_code varchar(200),
                hashkey varchar(200),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(2),
                file_name varchar(100),
                run_id number(14,0),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                business_units_id varchar(200),
                business_unit_name varchar(200),
                territory_or_region varchar(200)

    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_pop6_pops
        {% else %}
            {{schema}}.ntaitg_integration__itg_pop6_pops
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}