{% macro build_dim_organization()%}

{% set query %}
        create table if not exists
        {% if target.name=='prod' %}
                    hcposeedw_integration.dim_organization
                {% else %}
                    {{schema}}.hcposeedw_integration__dim_organization
                {% endif %}
            (organization_key varchar(32),
	        territory_source_id varchar(18),
	        country_code varchar(8),
            my_organization_code varchar(18),
            my_organization_name varchar(80),
            organization_l1_code varchar(18),
            organization_l1_name varchar(80),
            organization_l2_code varchar(18),
            organization_l2_name varchar(80),
            organization_l3_code varchar(18),
            organization_l3_name varchar(80),
            organization_l4_code varchar(18),
            organization_l4_name varchar(80),
            organization_l5_code varchar(18),
            organization_l5_name varchar(80),
            organization_l6_code varchar(18),
            organization_l6_name varchar(80),
            organization_l7_code varchar(18),
            organization_l7_name varchar(80),
            organization_l8_code varchar(18),
            organization_l8_name varchar(80),
            organization_l9_code varchar(18),
            organization_l9_name varchar(80),
            effective_from_date date,
            effective_to_date date,
            flag varchar(18),
            inserted_date timestamp_ntz(9),
            updated_date timestamp_ntz(9)
    );  
     create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            hcposeedw_integration.dim_organization
        {% else %}
            {{schema}}.hcposeedw_integration__dim_organization
        {% endif %};
{% endset %}

    {% do run_query(query) %}

{% endmacro%}