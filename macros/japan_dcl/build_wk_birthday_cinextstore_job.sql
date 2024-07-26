{% macro build_wk_birthday_cinextstore_job() %}
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.wk_birthday_cinextstore_job
                {% else %}
                    {{schema}}.jpndcledw_integration__wk_birthday_cinextstore_job
                {% endif %}
         (
                DIUSRID NUMBER(10,0) NOT NULL,
                DIORDERCNT NUMBER(10,0),
                C_DSSHUKKADATE VARCHAR(8),
                JUDGEKBN VARCHAR(1),
                DIMONTH VARCHAR(3)
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
    
{% endmacro %}