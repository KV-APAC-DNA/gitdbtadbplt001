{% macro build_itg_pop6_products_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                aspitg_integration.itg_pop6_products_temp
            {% else %}
                {{schema}}.aspitg_integration__itg_pop6_products_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    aspitg_integration.itg_pop6_products
                {% else %}
                    {{schema}}.aspitg_integration__itg_pop6_products
                {% endif %}
    (       	cntry_cd varchar(10),
                src_file_date varchar(10),
                status number(18,0),
                productdb_id varchar(255),
                barcode varchar(150),
                sku varchar(150),
                unit_price number(18,2),
                display_order number(18,0),
                launch_date varchar(20),
                largest_uom_quantity number(18,0),
                middle_uom_quantity number(18,0),
                smallest_uom_quantity number(18,0),
                company varchar(200),
                sku_english varchar(200),
                sku_code varchar(200),
                ps_category varchar(200),
                ps_segment varchar(200),
                ps_category_segment varchar(200),
                country_l1 varchar(200),
                regional_franchise_l2 varchar(200),
                franchise_l3 varchar(200),
                brand_l4 varchar(200),
                sub_category_l5 varchar(200),
                platform_l6 varchar(200),
                variance_l7 varchar(200),
                pack_size_l8 varchar(200),
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
            aspitg_integration.itg_pop6_products
        {% else %}
            {{schema}}.aspitg_integration__itg_pop6_products
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}