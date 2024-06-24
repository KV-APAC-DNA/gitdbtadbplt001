{% macro build_itg_salesmanmaster_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                inditg_integration.itg_salesmanmaster_temp
            {% else %}
                {{schema}}.inditg_integration__itg_salesmanmaster_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    inditg_integration.itg_salesmanmaster
                {% else %}
                    {{schema}}.inditg_integration__itg_salesmanmaster
                {% endif %}
    (   
        distcode varchar(100),
        smid number(18,0),
        smcode varchar(100),
        smname varchar(100),
        smphoneno varchar(100),
        smemail varchar(100),
        smotherdetails varchar(500),
        smdailyallowance number(38,6),
        smmonthlysalary number(38,6),
        smmktcredit number(38,6),
        smcreditdays number(18,0),
        status varchar(20),
        rmid number(18,0),
        rmcode varchar(100),
        rmname varchar(100),
        uploadflag varchar(1),
        createddate timestamp_ntz(9),
        syncid number(38,0),
        rdssmtype varchar(100),
        uniquesalescode varchar(15),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9),
        file_rec_dt date
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            inditg_integration.itg_salesmanmaster
        {% else %}
            {{schema}}.inditg_integration__itg_salesmanmaster
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
