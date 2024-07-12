{% macro build_wk_birthday_cinextbioptron() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.wk_birthday_cinextbioptron
                {% else %}
                    {{schema}}.jpndcledw_integration__wk_birthday_cinextbioptron
                {% endif %}
         (
                DIUSRID NUMBER(10,0) NOT NULL,
                DIORDERCNT NUMBER(10,0),
                C_DSSHUKKADATE VARCHAR(32),
                JUDGEKBN VARCHAR(4),
                DIMONTH VARCHAR(10)
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}