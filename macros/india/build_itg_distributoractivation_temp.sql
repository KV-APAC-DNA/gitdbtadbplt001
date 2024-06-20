{% macro build_itg_distributoractivation_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                inditg_integration.itg_distributoractivation_temp
            {% else %}
                {{schema}}.inditg_integration__itg_distributoractivation_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    inditg_integration.itg_distributoractivation
                {% else %}
                    {{schema}}.inditg_integration__itg_distributoractivation
                {% endif %}
    (   
        distcode varchar(400),
        activefromdate timestamp_ntz(9),
        activatedby number(18,0),
        activatedon timestamp_ntz(9),
        inactivefromdate timestamp_ntz(9),
        inactivatedby number(18,0),
        inactivatedon timestamp_ntz(9),
        activestatus number(18,0),
        createddate timestamp_ntz(9),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            inditg_integration.itg_distributoractivation
        {% else %}
            {{schema}}.inditg_integration__itg_distributoractivation
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
