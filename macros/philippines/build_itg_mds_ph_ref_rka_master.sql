{% macro build_itg_mds_ph_ref_rka_master() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_ref_rka_master
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_ref_rka_master
                {% endif %}
        (
                    rka_cd varchar(255),
                    rka_nm varchar(255),
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



