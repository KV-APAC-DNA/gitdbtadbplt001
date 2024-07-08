{% macro build_itg_pop6_product_groups_lists() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    aspitg_integration.itg_pop6_product_groups_lists
                {% else %}
                    {{schema}}.aspitg_integration__itg_pop6_product_groups_lists
                {% endif %}
         (
                    cntry_cd varchar(10),
                    src_file_date varchar(10),
                    product_group_status number(18,0),
                    product_group varchar(25),
                    product_list_status number(18,0),
                    product_list varchar(30),
                    productdb_id varchar(255),
                    sku varchar(150),
                    prod_grp_date date,
                    custom_pop_attribute_1 varchar(200),
                    custom_pop_attribute_2 varchar(200),
                    custom_pop_attribute_3 varchar(200),
                    hashkey varchar(200),
                    effective_from timestamp_ntz(9),
                    effective_to timestamp_ntz(9),
                    active varchar(2),
                    file_name varchar(100),
                    run_id number(14,0),
                    crtd_dttm timestamp_ntz(9),
                    updt_dttm timestamp_ntz(9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
