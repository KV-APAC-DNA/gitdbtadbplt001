{% macro build_itg_vn_mt_pos_price_products() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    vnmitg_integration.itg_vn_mt_pos_price_products
                {% else %}
                    {{schema}}.vnmitg_integration__itg_vn_mt_pos_price_products
                {% endif %}
        (
                chain varchar(200),
                changetrackingmask number(18,0),
                code varchar(500),
                customer_code varchar(200),
                customer_name varchar(200),
                customer_store_code varchar(200),
                district varchar(200),
                enterdatetime timestamp_ntz(9),
                enterusername varchar(200),
                enterversionnumber number(18,0),
                format varchar(200),
                id number(18,0),
                lastchgdatetime timestamp_ntz(9),
                lastchgusername varchar(200),
                lastchgversionnumber number(18,0),
                muid varchar(36),
                name varchar(500),
                note_closed_store varchar(200),
                plant varchar(200),
                status varchar(200),
                store_code varchar(200),
                store_name varchar(200),
                store_name_2 varchar(200),
                validationstatus varchar(500),
                version_id number(18,0),
                versionflag varchar(100),
                versionname varchar(100),
                versionnumber number(18,0),
                wh varchar(200),
                zone varchar(200),
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



