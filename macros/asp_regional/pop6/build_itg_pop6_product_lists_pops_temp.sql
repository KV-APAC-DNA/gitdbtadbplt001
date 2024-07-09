{% macro build_itg_pop6_product_lists_pops_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                aspitg_integration.itg_pop6_product_lists_pops_temp
            {% else %}
                {{schema}}.aspitg_integration__itg_pop6_product_lists_pops_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    aspitg_integration.itg_pop6_product_lists_pops
                {% else %}
                    {{schema}}.aspitg_integration__itg_pop6_product_lists_pops
                {% endif %}
    (       	cntry_cd varchar(10),
                src_file_date varchar(10),
                product_list varchar(100),
                popdb_id varchar(255),
                pop_code varchar(50),
                pop_name varchar(100),
                prod_grp_date date,
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
            aspitg_integration.itg_pop6_product_lists_pops
        {% else %}
            {{schema}}.aspitg_integration__itg_pop6_product_lists_pops
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}