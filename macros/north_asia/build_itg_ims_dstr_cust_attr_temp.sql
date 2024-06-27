{% macro build_itg_ims_dstr_cust_attr_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_ims_dstr_cust_attr_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_ims_dstr_cust_attr_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_ims_dstr_cust_attr
                {% else %}
                    {{schema}}.ntaitg_integration__itg_ims_dstr_cust_attr
                {% endif %}
    (           dstr_cd varchar(10),
                dstr_nm varchar(20),
                dstr_cust_cd varchar(50),
                dstr_cust_nm varchar(50),
                dstr_cust_area varchar(20),
                dstr_cust_clsn1 varchar(100),
                dstr_cust_clsn2 varchar(100),
                dstr_cust_clsn3 varchar(100),
                dstr_cust_clsn4 varchar(100),
                dstr_cust_clsn5 varchar(100),
                ctry_cd varchar(3),
                crt_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                distributor_address varchar(255),
                distributor_telephone varchar(255),
                distributor_contact varchar(255),
                store_type varchar(255),
                hq varchar(255)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_ims_dstr_cust_attr
        {% else %}
            {{schema}}.ntaitg_integration__itg_ims_dstr_cust_attr
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}