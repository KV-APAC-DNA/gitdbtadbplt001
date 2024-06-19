{% macro build_itg_pop6_users() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pop6_users
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pop6_users
                {% endif %}
            (
                cntry_cd varchar(10),
                src_file_date varchar(10),
                status number(18,0),
                userdb_id varchar(255),
                username varchar(50),
                first_name varchar(50),
                last_name varchar(50),
                team varchar(50),
                superior_name varchar(50),
                authorisation_group varchar(50),
                email_address varchar(50),
                longitude number(18,5),
                latitude number(18,5),
                hashkey varchar(200),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(2),
                file_name varchar(100),
                run_id number(14,0),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                business_units_id varchar(200),
                business_unit_name varchar(200)
        );              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}