{% macro build_edw_customer_attr_flat_dim_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaedw_integration.edw_customer_attr_flat_dim_temp
            {% else %}
                {{schema}}.ntaedw_integration__edw_customer_attr_flat_dim_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaedw_integration.edw_customer_attr_flat_dim
                {% else %}
                    {{schema}}.ntaedw_integration__edw_customer_attr_flat_dim
                {% endif %}
(       aw_remote_key varchar(100),
        cust_nm varchar(100),
        street_num varchar(100),
        street_nm varchar(500),
        city varchar(100),
        post_cd varchar(100),
        dist varchar(100),
        county varchar(100),
        cntry varchar(100),
        ph_num varchar(100),
        email_id varchar(100),
        website varchar(100),
        store_typ varchar(100),
        cust_store_ref varchar(100),
        channel varchar(100),
        sls_grp varchar(100),
        secondary_trade_cd varchar(100),
        secondary_trade_nm varchar(100),
        sold_to_party varchar(100),
        trgt_type varchar(50),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9),
        crt_ds varchar(10),
        upd_ds varchar(10),
        sls_ofc varchar(4),
        sls_ofc_desc varchar(40),
        sls_grp_cd varchar(3),
        sfa_cust_code varchar(20)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaedw_integration.edw_customer_attr_flat_dim
        {% else %}
            {{schema}}.ntaedw_integration__edw_customer_attr_flat_dim
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}