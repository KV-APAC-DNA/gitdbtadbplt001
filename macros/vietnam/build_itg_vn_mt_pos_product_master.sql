{% macro build_itg_vn_mt_pos_product_master() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    vnmitg_integration.itg_vn_mt_pos_product_master
                {% else %}
                    {{schema}}.vnmitg_integration__itg_vn_mt_pos_product_master
                {% endif %}
        (
                
                barcode varchar(200),
                changetrackingmask number(18,0),
                code varchar(500),
                customer varchar(200),
                customer_sku varchar(200),
                enterdatetime timestamp_ntz(9),
                enterusername varchar(200),
                enterversionnumber number(18,0),
                id number(18,0),
                lastchgdatetime timestamp_ntz(9),
                lastchgusername varchar(200),
                lastchgversionnumber number(18,0),
                muid varchar(36),
                name varchar(500),
                validationstatus varchar(500),
                version_id number(18,0),
                versionflag varchar(100),
                versionname varchar(100),
                versionnumber number(18,0),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(2),
                run_id number(14,0),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



