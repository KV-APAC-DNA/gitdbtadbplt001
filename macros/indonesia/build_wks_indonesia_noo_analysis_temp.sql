{% macro build_wks_indonesia_noo_analysis_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                idnwks_integration.wks_indonesia_noo_analysis_temp
            {% else %}
                {{schema}}.idnwks_integration__wks_indonesia_noo_analysis_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    idnwks_integration.wks_indonesia_noo_analysis
                {% else %}
                    {{schema}}.idnwks_integration__wks_indonesia_noo_analysis
                {% endif %}
    (       	jj_year number(18,0),
                jj_qrtr varchar(24),
                jj_mnth varchar(25),
                jj_wk number(18,0),
                jj_mnth_wk_no number(38,0),
                jj_mnth_no number(18,0),
                bill_doc varchar(100),
                bill_dt timestamp_ntz(9),
                dstrbtr_grp_cd varchar(25),
                dstrbtr_grp_nm varchar(155),
                jj_sap_dstrbtr_id varchar(75),
                jj_sap_dstrbtr_nm varchar(75),
                dstrbtr_cd_nm varchar(152),
                area varchar(50),
                region varchar(50),
                bdm_nm varchar(50),
                rbm_nm varchar(50),
                dstrbtr_status varchar(50),
                cust_id_map varchar(100),
                cust_nm_map varchar(100),
                dstrbtr_cust_cd_nm varchar(304),
                cust_grp varchar(100),
                chnl varchar(100),
                outlet_type varchar(100),
                chnl_grp varchar(100),
                jjid varchar(100),
                chnl_grp2 varchar(100),
                city varchar(229),
                cust_status varchar(8),
                jj_sap_prod_id varchar(50),
                jj_sap_prod_desc varchar(100),
                jj_sap_upgrd_prod_id varchar(50),
                jj_sap_upgrd_prod_desc varchar(100),
                jj_sap_cd_mp_prod_id varchar(50),
                jj_sap_cd_mp_prod_desc varchar(100),
                sap_prod_code_name varchar(152),
                franchise varchar(50),
                brand varchar(50),
                variant1 varchar(50),
                variant2 varchar(50),
                variant varchar(50),
                put_up varchar(62),
                prod_status varchar(50),
                slsmn_id varchar(100),
                slsmn_nm varchar(100),
                sls_qty number(18,4),
                hna number(18,4),
                niv number(18,4),
                trd_dscnt number(18,4),
                dstrbtr_niv number(18,4),
                rtrn_qty number(18,4),
                rtrn_val number(18,4),
                hsku_target_growth number(18,4),
                hsku_target_coverage number(18,4),
                jj_mnth_long varchar(10),
                trgt_hna number(38,4),
                trgt_niv number(38,4),
                npi_flag varchar(1),
                benchmark_sku_code varchar(75),
                sku_benchmark varchar(75),
                hero_sku_flag varchar(1),
                trgt_dist_brnd_chnl_flag varchar(1),
                tiering varchar(100),
                count_sku_code number(18,0),
                mcs_status varchar(20),
                local_variant varchar(2000),
                count_local_variant number(18,0),
                salesman_key varchar(70),
                sfa_id varchar(255),
                latest_chnl varchar(100),
                latest_outlet_type varchar(100),
                latest_chnl_grp varchar(100),
                latest_cust_grp2 varchar(100),
                latest_cust_grp varchar(100),
                latest_cust_nm_map varchar(100),
                latest_region varchar(100),
                latest_area varchar(100),
                latest_rbm varchar(100),
                latest_area_pic varchar(100),
                latest_jjid varchar(200),
                latest_put_up varchar(200),
                latest_franchise varchar(200),
                latest_brand varchar(200),
                latest_msl varchar(200),
                latest_count_local_variant varchar(200),
                latest_chnl_grp2 varchar(200),
                latest_distributor_group varchar(200),
                latest_dstrbtr_grp_cd varchar(200)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            idnwks_integration.wks_indonesia_noo_analysis
        {% else %}
            {{schema}}.idnwks_integration__wks_indonesia_noo_analysis
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}