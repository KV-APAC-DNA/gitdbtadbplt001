{% macro build_itg_mds_ph_ref_parent_customer() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_ref_parent_customer
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_ref_parent_customer
                {% endif %}
            (
                parent_cust_cd varchar(30),
                parent_cust_nm varchar(255),
                rpt_grp_11 varchar(50),
                rpt_grp_11_desc varchar(255),
                rpt_grp_12 varchar(50),
                rpt_grp_12_desc varchar(255),
                rpt_grp_1 varchar(50),
                rpt_grp_1_desc varchar(255),
                rpt_grp_2 varchar(50),
                rpt_grp_2_desc varchar(255),
                last_chg_datetime timestamp_ntz(9),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(10),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                customer_segmentation1 varchar(256),
                customer_segmentation2 varchar(256),
                channel varchar(256)
        );              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



