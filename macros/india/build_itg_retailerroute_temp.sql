{% macro build_itg_retailerroute_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                inditg_integration.itg_retailerroute_temp
            {% else %}
                {{schema}}.inditg_integration__itg_retailerroute_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    inditg_integration.itg_retailerroute
                {% else %}
                    {{schema}}.inditg_integration__itg_retailerroute
                {% endif %}
    (   
        distcode varchar(100),
        rtrcode varchar(100),
        rtrname varchar(100),
        rmcode varchar(100),
        rmname varchar(100),
        routetype varchar(100),
        uploadflag varchar(10),
        createddate timestamp_ntz(9),
        syncid number(38,0),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9),
        file_rec_dt date
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            inditg_integration.itg_retailerroute
        {% else %}
            {{schema}}.inditg_integration__itg_retailerroute
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
