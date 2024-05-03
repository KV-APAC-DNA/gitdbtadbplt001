{% macro build_itg_mds_ph_lav_customer() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_lav_customer
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_lav_customer
                {% endif %}
         (
                		cust_id varchar(50),
                        cust_nm varchar(255),
                        parent_cust_cd varchar(50),
                        parent_cust_nm varchar(255),
                        dstrbtr_grp_cd varchar(50),
                        dstrbtr_grp_nm varchar(255),
                        region_cd varchar(50),
                        region_nm varchar(255),
                        province_cd varchar(50),
                        province_nm varchar(255),
                        mun_cd varchar(50),
                        mun_nm varchar(255),
                        channel_cd varchar(50),
                        channel_desc varchar(255),
                        sub_chnl_cd varchar(50),
                        sub_chnl_desc varchar(255),
                        rpt_grp_3_cd varchar(50),
                        rpt_grp_3_desc varchar(255),
                        rpt_grp_4_cd varchar(50),
                        rpt_grp_4_desc varchar(255),
                        rpt_grp_5_cd varchar(50),
                        rpt_grp_5_desc varchar(255),
                        rpt_grp_6_cd varchar(50),
                        rpt_grp_6_desc varchar(255),
                        rpt_grp_7_cd varchar(50),
                        rpt_grp_7_desc varchar(255),
                        rpt_grp_9_cd varchar(50),
                        rpt_grp_9_desc varchar(255),
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
                        latitude number(20,10),
                        longitude number(20,10),
                        long_lat_source number(20,10)
        );

    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



