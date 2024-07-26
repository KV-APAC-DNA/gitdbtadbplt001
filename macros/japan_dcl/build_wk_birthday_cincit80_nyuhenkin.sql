{% macro build_wk_birthday_cincit80_nyuhenkin() %}
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.wk_birthday_cit80_nyuhenkin
                {% else %}
                    {{schema}}.jpndcledw_integration__cit80_nyuhenkin
                {% endif %}
         (
                SAL_NO VARCHAR(18) NOT NULL,
                SKY_KIN NUMBER(38,0) DEFAULT 0,
                SOUSAI_KSSAI_KIN NUMBER(38,0) DEFAULT 0,
                KAIS_KSKM_KIN NUMBER(38,0) DEFAULT 0,
                JIKAI_SKY_KURKOSI_KIN NUMBER(38,0) DEFAULT 0,
                MI_KAIS_KIN NUMBER(38,0) DEFAULT 0,
                NYUHENKANFLG NUMBER(38,0) DEFAULT 0,
                INSERTDATE TIMESTAMP_NTZ(9),
                UPDATEDATE TIMESTAMP_NTZ(9),
                INSERTED_DATE TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_TZ(9)),
                INSERTED_BY VARCHAR(100),
                UPDATED_DATE TIMESTAMP_NTZ(9) DEFAULT CAST('CURRENT_TIMESTAMP()' AS TIMESTAMP_TZ(9)),
                UPDATED_BY VARCHAR(100)
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
    
{% endmacro %}