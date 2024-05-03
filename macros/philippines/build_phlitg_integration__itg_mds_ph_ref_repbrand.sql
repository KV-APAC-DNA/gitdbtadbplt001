{% macro build_phlitg_integration__itg_mds_ph_ref_repbrand() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_ref_repbrand
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_ref_repbrand
                {% endif %}
         (
            brand_id varchar(30),
            brand_cd varchar(30),
            brand_nm varchar(255),
            franchise_cd varchar(50),
            franchise_id varchar(50),
            francise_nm varchar(255),
            local_branc_desc varchar(255),
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



