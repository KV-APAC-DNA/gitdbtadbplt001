{% macro build_itg_vn_mt_customer_sales_organization() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    vnmitg_integration.itg_vn_mt_customer_sales_organization
                {% else %}
                    {{schema}}.vnmitg_integration__itg_vn_mt_customer_sales_organization
                {% endif %}
         (
                address varchar(1200),
                code varchar(500),
                code_sr_pg varchar(60),
                code_ss varchar(60),
                customer_name varchar(600),
                district_name varchar(200),
                dksh_jnj varchar(200),
                id number(18,0),
                kam varchar(200),
                mtd_code number(31,0),
                mti_code number(31,0),
                name varchar(500),
                rom varchar(60),
                sales_man varchar(400),
                sales_supervisor varchar(200),
                status varchar(60),
                changetrackingmask number(18,0),
                enterdatetime timestamp_ntz(9),
                enterusername varchar(200),
                enterversionnumber number(18,0),
                lastchgdatetime timestamp_ntz(9),
                lastchgusername varchar(200),
                lastchgversionnumber number(18,0),
                muid varchar(36),
                validationstatus varchar(500),
                version_id number(18,0),
                versionflag varchar(100),
                versionname varchar(100),
                versionnumber number(18,0),
                effective_from date,
                effective_to date,
                active varchar(2),
                run_id number(14,0),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



