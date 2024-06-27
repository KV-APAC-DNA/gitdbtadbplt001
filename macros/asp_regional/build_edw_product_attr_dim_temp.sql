
{% macro build_edw_product_attr_dim_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                aspedw_integration.edw_product_attr_dim_temp
            {% else %}
                {{schema}}.aspedw_integration__edw_product_attr_dim_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    aspedw_integration.edw_product_attr_dim
                {% else %}
                    {{schema}}.aspedw_integration__edw_product_attr_dim
                {% endif %}
    (       	aw_remote_key varchar(100),
                awrefs_prod_remotekey varchar(100),
                awrefs_buss_unit varchar(100),
                sap_matl_num varchar(100),
                cntry varchar(10) not null,
                ean varchar(20) not null,
                prod_hier_l1 varchar(500),
                prod_hier_l2 varchar(500),
                prod_hier_l3 varchar(500),
                prod_hier_l4 varchar(500),
                prod_hier_l5 varchar(500),
                prod_hier_l6 varchar(500),
                prod_hier_l7 varchar(500),
                prod_hier_l8 varchar(500),
                prod_hier_l9 varchar(500),
                crt_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                lcl_prod_nm varchar(100),
                primary key (ean, cntry)

    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            aspedw_integration.edw_product_attr_dim
        {% else %}
            {{schema}}.aspedw_integration__edw_product_attr_dim
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}