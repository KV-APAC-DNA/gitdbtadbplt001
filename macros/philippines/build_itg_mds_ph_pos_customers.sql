{% macro build_itg_mds_ph_pos_customers() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_pos_customers
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_pos_customers
                {% endif %}
        (
                		code varchar(50),
                        cust_cd varchar(50),
                        brnch_cd varchar(50),
                        brnch_nm varchar(255),
                        prov_cd varchar(50),
                        prov_nm varchar(255),
                        region_cd varchar(50),
                        region_nm varchar(255),
                        mncplty_cd varchar(50),
                        mncplty_nm varchar(255),
                        city_cd varchar(50),
                        city_nm varchar(255),
                        chnl_sub_grp_cd varchar(50),
                        pms_nm varchar(255),
                        ash_no varchar(255),
                        ash_nm varchar(255),
                        address1 varchar(255),
                        address2 varchar(255),
                        jj_chnl_cd varchar(255),
                        tsr varchar(255),
                        pmr varchar(255),
                        chnl varchar(255),
                        area_cd varchar(255),
                        area_nm varchar(255),
                        region_nm2 varchar(255),
                        wsman varchar(255),
                        route varchar(255),
                        status varchar(255),
                        operating_hrs varchar(255),
                        da_tag varchar(255),
                        dizr_tag varchar(255),
                        brnch_grp1 varchar(255),
                        brnch_grp2 varchar(255),
                        outlet_typ varchar(255),
                        store_typ_cd varchar(255),
                        store_typ_desc varchar(255),
                        prov_cd2 varchar(255),
                        data_compln_status varchar(255),
                        ae_nm varchar(255),
                        dt_opnd varchar(255),
                        store_tin_no varchar(255),
                        store_size_cd varchar(255),
                        store_size varchar(255),
                        store_mtrx_cd varchar(255),
                        store_mtrx varchar(255),
                        class varchar(255),
                        store_clsfn varchar(255),
                        jj_sold_to varchar(255),
                        jj_ship_to varchar(255),
                        bp varchar(255),
                        sales_grp varchar(255),
                        dsm varchar(255),
                        sales_man_tag varchar(255),
                        branch_no varchar(255),
                        mdc_tag varchar(255),
                        derived_branchno varchar(255),
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
                        long_lat_source number(20,10),
                        latitude number(20,10),
                        longitude number(20,10)        
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



