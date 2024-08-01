{% macro build_da_iv_edi_accum_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpnedw_integration.da_iv_edi_accum_temp
            {% else %}
                {{schema}}.jpnedw_integration__da_iv_edi_accum_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnedw_integration.da_iv_edi_accum
                {% else %}
                    {{schema}}.jpnedw_integration__da_iv_edi_accum
                {% endif %}
    (       		
           	create_dt timestamp_ntz(9),
            create_user varchar(20),
            update_dt timestamp_ntz(9),
            update_user varchar(20),
            cstm_cd varchar(10),
            item_cd varchar(18),
            invt_dt timestamp_ntz(9),
            cs_qty number(6,0),
            qty number(6,0),
            proc_dt timestamp_ntz(9),
            van_type varchar(10),
            jan_cd varchar(18),
            exec_dt timestamp_ntz(9),
            valid_flg varchar(1)

    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            jpnedw_integration.da_iv_edi_accum
        {% else %}
            {{schema}}.jpnedw_integration__da_iv_edi_accum
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}