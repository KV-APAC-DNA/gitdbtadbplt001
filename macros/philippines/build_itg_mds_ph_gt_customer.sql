{% macro build_itg_mds_ph_gt_customer() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_gt_customer
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_gt_customer
                {% endif %}
            (
                dstrbtr_cust_id varchar(50),
                dstrbtr_cust_nm varchar(255),
                slsmn varchar(50),
                slsmn_desc varchar(255),
                rep_grp2 varchar(50),
                rep_grp2_desc varchar(255),
                rep_grp3 varchar(50),
                rep_grp3_desc varchar(255),
                rep_grp4 varchar(50),
                rep_grp4_desc varchar(255),
                rep_grp5 varchar(50),
                rep_grp5_desc varchar(255),
                rep_grp6 varchar(50),
                rep_grp6_desc varchar(255),
                status varchar(50),
                address varchar(255),
                zip varchar(50),
                slm_grp_cd varchar(255),
                frequency_visit varchar(50),
                store_prioritization varchar(255),
                latitude varchar(50),
                longitude varchar(255),
                rpt_grp9 varchar(50),
                rpt_grp9_desc varchar(255),
                rpt_grp11 varchar(50),
                rpt_grp11_desc varchar(255),
                sls_dist varchar(50),
                sls_dist_desc varchar(255),
                dstrbtr_grp_cd varchar(50),
                dstrbtr_grp_nm varchar(255),
                rpt_grp_15_desc varchar(255),
                last_chg_datetime timestamp_ntz(9),
                effective_from timestamp_ntz(9),
                effective_to timestamp_ntz(9),
                active varchar(10),
                crtd_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9),
                zip_code varchar(100),
                zip_cd_name varchar(255),
                barangay_code varchar(100),
                barangay_cd_name varchar(255),
	            long_lat_source number(20,10)
        );              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



