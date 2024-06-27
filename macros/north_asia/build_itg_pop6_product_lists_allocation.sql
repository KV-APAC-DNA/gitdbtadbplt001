{% macro build_itg_pop6_product_lists_allocation() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pop6_product_lists_allocation
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pop6_product_lists_allocation
                {% endif %}
            (
                	cntry_cd varchar(10),
                    src_file_date varchar(10),
                    product_group_status number(18,0),
                    product_group varchar(25),
                    product_list_status number(18,0),
                    product_list varchar(100),
                    pop_attribute_id varchar(255),
                    pop_attribute varchar(200),
                    pop_attribute_value_id varchar(255),
                    pop_attribute_value varchar(200),
                    prod_grp_date date,
                    hashkey varchar(200),
                    effective_from timestamp_ntz(9),
                    effective_to timestamp_ntz(9),
                    active varchar(2),
                    file_name varchar(100),
                    run_id number(14,0),
                    crtd_dttm timestamp_ntz(9),
                    updt_dttm timestamp_ntz(9),
                    product_list_code varchar(100)
        );              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}