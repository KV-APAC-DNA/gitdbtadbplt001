{% macro build_wk_birthday_cinextusrpram() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.wk_birthday_cinextusrpram
                {% else %}
                    {{schema}}.jpndcledw_integration__wk_birthday_cinextusrpram
                {% endif %}
         (
                
                DIUSRID NUMBER(10,0) NOT NULL,
                DSDAT60 VARCHAR(4000),
                DSDAT61 VARCHAR(4000),
                SHUKKADT_ROUTEID VARCHAR(400),
                DISECESSIONFLG VARCHAR(4),
                DIELIMFLG VARCHAR(4) DEFAULT CAST(0 AS VARCHAR(1)),
                JUDGEKBN VARCHAR(4),
                DIMONTH VARCHAR(32)
        );                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}