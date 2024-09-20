{% macro build_wk_birthday_viewcolumn_job() %}
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.wk_birthday_viewcolumn_job
                {% else %}
                    {{schema}}.jpndcledw_integration__wk_birthday_viewcolumn_job
                {% endif %}
         (
                DIUSRID NUMBER(10,0) NOT NULL,
                DSUSRKBN VARCHAR(6),
                DSSHUKKADATE VARCHAR(36),
                JUDGEKBN VARCHAR(2),
                DIMONTH VARCHAR(3)
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
    
{% endmacro %}