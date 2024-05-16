{% macro build_itg_pos_prom_prc_map_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_pos_prom_prc_map_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_pos_prom_prc_map_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pos_prom_prc_map
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pos_prom_prc_map
                {% endif %}
    (            
            cust varchar(20),
            barcd varchar(20),
            cust_prod_cd varchar(20),
            prom_prc number(30,4),
            prom_strt_dt date,
            prom_end_dt date,
            crt_dttm timestamp_ntz(9),
            updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_pos_prom_prc_map
        {% else %}
            {{schema}}.ntaitg_integration__itg_pos_prom_prc_map
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}