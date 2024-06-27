{% macro built_itg_mds_in_sv_winculum_master() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    inditg_integration.itg_mds_in_sv_winculum_master
                {% else %}
                    {{schema}}.inditg_integration__itg_mds_in_sv_winculum_master
                {% endif %}
         (
            customercode varchar(200),
            customername varchar(200),
            regioncode varchar(200),
            zonecode varchar(200),
            territorycode varchar(200),
            statecode varchar(200),
            towncode varchar(200),
            psnonps varchar(200),
            isactive varchar(200),
            wholesalercode varchar(200),
            parentcustomercode varchar(200),
            isdirectacct varchar(200),
            abicode varchar(200),
            distributorsapid varchar(200),
            name varchar(500),
            code varchar(500),
            changetrackingmask number(18,0),
            validationstatus varchar(500),
            version_id number(18,0),
            versionflag varchar(100),
            versionname varchar(100),
            versionnumber number(18,0),
            lastchgdatetime timestamp_ntz(9),
            lastchgusername varchar(200),
            lastchgversionnumber number(18,0),
            effective_from timestamp_ntz(9),
            effective_to timestamp_ntz(9),
            active varchar(2),
            run_id number(18,0),
            crtd_dttm timestamp_ntz(9),
            updt_dttm timestamp_ntz(9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



