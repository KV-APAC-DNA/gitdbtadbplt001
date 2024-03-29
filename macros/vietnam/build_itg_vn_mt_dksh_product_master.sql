{% macro build_itg_vn_mt_dksh_product_master() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    vnmitg_integration.itg_vn_mt_dksh_product_master
                {% else %}
                    {{schema}}.vnmitg_integration__itg_vn_mt_dksh_product_master
                {% endif %}
         (
                	id number(18,0),
                    muid varchar(36),
                    versionname varchar(100),
                    versionnumber number(18,0),
                    version_id number(18,0),
                    versionflag varchar(100),
                    name varchar(500),
                    code varchar(500),
                    changetrackingmask number(18,0),
                    barcode number(31,0),
                    jnj_sap_code number(31,0),
                    enterdatetime timestamp_ntz(9),
                    enterusername varchar(200),
                    enterversionnumber number(18,0),
                    lastchgdatetime timestamp_ntz(9),
                    lastchgusername varchar(200),
                    lastchgversionnumber number(18,0),
                    validationstatus varchar(500),
                    base_bundle varchar(500),
                    category varchar(500),
                    franchise varchar(500),
                    product_name varchar(500),
                    size varchar(400),
                    sub_brand varchar(400),
                    sub_category varchar(500),
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



