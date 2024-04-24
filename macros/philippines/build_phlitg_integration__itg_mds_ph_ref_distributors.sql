{% macro build_phlitg_integration__itg_mds_ph_ref_distributors() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_ref_distributors
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_ref_distributors
                {% endif %}
         (
            dstrbtr_grp_cd varchar(20),
            dstrbtr_grp_nm varchar(255),
            primary_sold_to varchar(50),
            primary_sold_to_nm varchar(255),
            rpt_grp_10 varchar(50),
            rpt_grp_10_desc varchar(255),
            rpt_grp_12 varchar(50),
            rpt_grp_12_desc varchar(255),
            rpt_grp_13 varchar(50),
            rpt_grp_13_desc varchar(255),
            rpt_grp_14 varchar(50),
            rpt_grp_14_desc varchar(255),
            rpt_grp_8 varchar(50),
            rpt_grp_8_desc varchar(255),
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



