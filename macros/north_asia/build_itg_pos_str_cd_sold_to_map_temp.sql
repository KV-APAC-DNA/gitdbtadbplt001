{% macro build_itg_pos_str_cd_sold_to_map_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_pos_str_cd_sold_to_map_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_pos_str_cd_sold_to_map_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pos_str_cd_sold_to_map
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pos_str_cd_sold_to_map
                {% endif %}
    (           clnt varchar(50),
                seqid varchar(50),
                str_nm varchar(100),
                src_sys_cd varchar(50),
                conv_sys_cd varchar(50),
                str_type varchar(50),
                cust_str_cd varchar(50),
                sold_to_cd varchar(50),
                crt_dttm timestamp_ntz(9),
                upd_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_pos_str_cd_sold_to_map
        {% else %}
            {{schema}}.ntaitg_integration__itg_pos_str_cd_sold_to_map
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}