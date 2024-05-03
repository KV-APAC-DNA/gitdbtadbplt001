{% macro build_itg_mds_ph_ref_pos_primary_sold_to() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_ref_pos_primary_sold_to
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_ref_pos_primary_sold_to
                {% endif %}
            (
                cust_cd varchar(30),
                primary_soldto varchar(30),
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



