{% macro build_mykokya_param() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    jpdcledw_integration.mykokya_param
                {% else %}
                    {{schema}}.jpndcledw_integration__mykokya_param
                {% endif %}
         (
                file_id number(18,0) not null,
                filename varchar(100),
                purpose_type varchar(30),
                upload_by varchar(50),
                customer_no_type varchar(30),
                source_file_date varchar(30),
                upload_dt varchar(10) default to_char(convert_timezone('utc', 'asia/tokyo', cast('current_timestamp()' as timestamp_ntz(9))), 'mm-dd-yyyy'),
                upload_time varchar(8) default to_char(convert_timezone('utc', 'asia/tokyo', cast('current_timestamp()' as timestamp_ntz(9))), 'hh24:mi:ss')
            
        );
                               
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}