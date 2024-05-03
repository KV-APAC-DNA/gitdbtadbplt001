{% macro build_itg_mds_ph_ref_repfranchise() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_ref_repfranchise
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_ref_repfranchise
                {% endif %}
         (
                	franchise_id varchar(30),
                    franchise_code varchar(50),
                    franchise_nm varchar(255),
                    last_chg_datetime timestamp_ntz(9),
                    effective_from timestamp_ntz(9),
                    effective_to timestamp_ntz(9),
                    active varchar(10),
                    crtd_dttm timestamp_ntz(9),
                    updt_dttm timestamp_ntz(9)
        );

    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



