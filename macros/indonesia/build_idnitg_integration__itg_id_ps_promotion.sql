{% macro build_idnitg_integration__itg_id_ps_promotion() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    idnitg_integration.itg_id_ps_promotion
                {% else %}
                    {{ schema }}.idnitg_integration__itg_id_ps_promotion
                {% endif %}
            (
                	outlet_id varchar(10),
					outlet_custom_code varchar(10),
					outlet_name varchar(100),
					province varchar(50),
					city varchar(50),
					channel varchar(50),
					merchandiser_id varchar(20),
					merchandiser_name varchar(50),
					cust_group varchar(50),
					address varchar(255),
					jnj_year varchar(4),
					jnj_month varchar(2),
					jnj_week varchar(5),
					day_name varchar(20),
					input_date date,
					promo_desc varchar(255),
					photo_link varchar(100),
					posm_execution_flag varchar(5),
					file_name varchar(100),
					crt_dttm timestamp_ntz(9)

        );

    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
