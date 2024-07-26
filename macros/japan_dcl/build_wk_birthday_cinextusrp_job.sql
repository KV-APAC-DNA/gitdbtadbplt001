{% macro build_wk_birthday_cinextusrp_job() %}
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.wk_birthday_cinextusrp_job
                {% else %}
                    {{schema}}.jpndcledw_integration__wk_birthday_cinextusrp_job
                {% endif %}
         (
                DIUSRID NUMBER(10,0) NOT NULL,
                DSDAT60 VARCHAR(6000),
                DSDAT61 VARCHAR(6000),
                SHUKKADT_ROUTEID VARCHAR(150),
                DISECESSIONFLG VARCHAR(1),
                DIELIMFLG VARCHAR(1) DEFAULT CAST(0 AS VARCHAR(1)),
                JUDGEKBN VARCHAR(1),
                DIMONTH VARCHAR(3)
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
    
{% endmacro %}