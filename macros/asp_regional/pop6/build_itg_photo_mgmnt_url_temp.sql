{% macro build_itg_photo_mgmnt_url_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                aspitg_integration.itg_photo_mgmnt_url_temp
            {% else %}
                {{schema}}.aspitg_integration__itg_photo_mgmnt_url_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    aspitg_integration.itg_photo_mgmnt_url
                {% else %}
                    {{schema}}.aspitg_integration__itg_photo_mgmnt_url
                {% endif %}
    (           original_photo_key varchar(1031),
                original_response varchar(65535),
                photo_key varchar(1053),
                response varchar(65535),
                url_cnt number(18,0),
                run_id number(14,0),
                create_dt timestamp_ntz(9),
                upload_photo_flag varchar(1)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            aspitg_integration.itg_photo_mgmnt_url
        {% else %}
            {{schema}}.aspitg_integration__itg_photo_mgmnt_url
        {% endif %};
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}