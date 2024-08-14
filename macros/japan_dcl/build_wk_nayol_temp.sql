{% macro build_wk_nayol_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpndcledw_integration.wk_nayol_temp
            {% else %}
                {{schema}}.jpndcledw_integration__wk_nayol_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpndcledw_integration.wk_nayol
                {% else %}
                    {{schema}}.jpndcledw_integration__wk_nayol
                {% endif %}
    (           nayosesakino varchar(15),
                kokyano varchar(15),
                insertdate varchar(12),
                shori_sts varchar(1),
                inserted_date timestamp_ntz(9),
                inserted_by varchar(100),
                updated_date timestamp_ntz(9),
                updated_by varchar(100)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            jpndcledw_integration.wk_nayol
        {% else %}
            {{schema}}.jpndcledw_integration__wk_nayol
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}