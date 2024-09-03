{% macro build_itg_sfmc_consumer_master_temp(filename) %}
    {% set tablename %}
    {% if target.name=='prod' %}
                phlitg_integration.itg_sfmc_consumer_master_temp
            {% else %}
                {{schema}}.phlitg_integration__itg_sfmc_consumer_master_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    aspitg_integration.itg_sfmc_consumer_master
                {% else %}
                    {{schema}}.aspitg_integration__itg_sfmc_consumer_master
                {% endif %}
        (
                            cntry_cd varchar(10),
                            first_name varchar(200),
                            last_name varchar(200),
                            mobile_num varchar(30),
                            mobile_cntry_cd varchar(10),
                            birthday_mnth varchar(10),
                            birthday_year varchar(10),
                            address_1 varchar(255),
                            address_2 varchar(255),
                            address_city varchar(100),
                            address_zipcode varchar(30),
                            subscriber_key varchar(100),
                            website_unique_id varchar(150),
                            source varchar(100),
                            medium varchar(100),
                            brand varchar(200),
                            address_cntry varchar(100),
                            campaign_id varchar(100),
                            created_date timestamp_ntz(9),
                            updated_date timestamp_ntz(9),
                            unsubscribe_date timestamp_ntz(9),
                            email varchar(100),
                            full_name varchar(200),
                            last_logon_time timestamp_ntz(9),
                            remaining_points number(10,4),
                            redeemed_points number(10,4),
                            total_points number(10,4),
                            gender varchar(20),
                            line_id varchar(50),
                            line_name varchar(200),
                            line_email varchar(100),
                            line_channel_id varchar(50),
                            address_region varchar(100),
                            tier varchar(100),
                            opt_in_for_communication varchar(100),
                            file_name varchar(255),
                            crtd_dttm timestamp_ntz(9),
                            updt_dttm timestamp_ntz(9),
                            have_kid varchar(20),
                            age number(18,0),
                            valid_from timestamp_ntz(9),
                            valid_to timestamp_ntz(9),
                            delete_flag varchar(10),
                            subscriber_status varchar(100),
                            opt_in_for_jnj_communication varchar(100),
                            opt_in_for_campaign varchar(100),
                            file_name varchar(255)

        );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            aspitg_integration.itg_sfmc_consumer_master
        {% else %}
            {{schema}}.aspitg_integration__itg_sfmc_consumer_master
        {% endif %};
        delete from {{tablename}}
        where file_name = '{{filename}}';
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}