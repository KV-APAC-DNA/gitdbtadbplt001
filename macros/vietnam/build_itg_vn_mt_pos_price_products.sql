{% macro build_itg_vn_mt_pos_price_products() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    vnmitg_integration.itg_vn_mt_pos_price_products
                {% else %}
                    {{schema}}.vnmitg_integration__itg_vn_mt_pos_price_products
                {% endif %}
        (
        	ID NUMBER(18,0),
            MUID VARCHAR(36),
            VERSIONNAME VARCHAR(100),
            VERSIONNUMBER NUMBER(18,0),
            VERSION_ID NUMBER(18,0),
            VERSIONFLAG VARCHAR(100),
            NAME VARCHAR(500),
            CODE VARCHAR(500),
            CHANGETRACKINGMASK NUMBER(18,0),
            JNJ_SAP_CODE NUMBER(31,0),
            FRANCHISE VARCHAR(60),
            BRAND VARCHAR(200),
            SKU VARCHAR(500),
            BAR_CODE VARCHAR(200),
            PC_PER_CASE NUMBER(31,2),
            PRICE NUMBER(31,5),
            PRODUCT_ID_CONCUNG VARCHAR(200),
            ENTERDATETIME TIMESTAMP_NTZ(9),
            ENTERUSERNAME VARCHAR(200),
            ENTERVERSIONNUMBER NUMBER(18,0),
            LASTCHGDATETIME TIMESTAMP_NTZ(9),
            LASTCHGUSERNAME VARCHAR(200),
            LASTCHGVERSIONNUMBER NUMBER(18,0),
            VALIDATIONSTATUS VARCHAR(500),
            EFFECTIVE_FROM TIMESTAMP_NTZ(9),
            EFFECTIVE_TO TIMESTAMP_NTZ(9),
            ACTIVE VARCHAR(2),
            RUN_ID NUMBER(14,0),
            CRTD_DTTM TIMESTAMP_NTZ(9),
            UPDT_DTTM TIMESTAMP_NTZ(9)

        );              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



