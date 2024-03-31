{% macro build_itg_vn_mt_dksh_cust_master() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    vnmitg_integration.itg_vn_mt_dksh_cust_master
                {% else %}
                    {{schema}}.vnmitg_integration__itg_vn_mt_dksh_cust_master
                {% endif %}
        (
                		account_code varchar(500),
                        account_id number(18,0),
                        account_name varchar(500),
                        address varchar(200),
                        changetrackingmask number(18,0),
                        code varchar(500),
                        enterdatetime timestamp_ntz(9),
                        enterusername varchar(200),
                        enterversionnumber number(18,0),
                        group_account_code varchar(500),
                        group_account_id number(18,0),
                        group_account_name varchar(500),
                        id number(18,0),
                        lastchgdatetime timestamp_ntz(9),
                        lastchgusername varchar(200),
                        lastchgversionnumber number(18,0),
                        muid varchar(36),
                        name varchar(500),
                        province_code varchar(500),
                        province_id number(18,0),
                        retail_environment varchar(200),
                        province_name varchar(500),
                        region_code varchar(500),
                        region_id number(18,0),
                        region_name varchar(500),
                        sub_channel_code varchar(500),
                        sub_channel_id number(18,0),
                        sub_channel_name varchar(500),
                        validationstatus varchar(500),
                        version_id number(18,0),
                        versionflag varchar(100),
                        versionname varchar(100),
                        versionnumber number(18,0),
                        ten_st_ddp varchar(200),
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



