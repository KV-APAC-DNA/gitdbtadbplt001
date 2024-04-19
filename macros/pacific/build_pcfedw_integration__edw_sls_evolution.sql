{% macro build_pcfedw_integration__edw_sls_evolution() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    pcfedw_integration.edw_sls_evolution
                {% else %}
                    {{schema}}.pcfedw_integration__edw_sls_evolution
                {% endif %}
         (
                	country varchar(20),
                    snapshot_date date,
                    cust_no varchar(10),
                    matl_id varchar(40),
                    grp_fran_desc varchar(100),
                    prod_fran_desc varchar(100),
                    prod_mjr_desc varchar(100),
                    prod_mnr_desc varchar(100),
                    matl_desc varchar(100),
                    brnd_desc varchar(100),
                    gcph_franchise varchar(100),
                    gcph_brand varchar(100),
                    gcph_subbrand varchar(100),
                    gcph_variant varchar(100),
                    gcph_needstate varchar(100),
                    gcph_category varchar(100),
                    gcph_subcategory varchar(100),
                    gcph_segment varchar(100),
                    gcph_subsegment varchar(100),
                    master_code varchar(18),
                    channel_desc varchar(20),
                    sales_office_desc varchar(30),
                    cust_nm varchar(100),
                    sales_grp_desc varchar(30),
                    key_measure varchar(40),
                    ciw_ctgry varchar(40),
                    ciw_accnt_grp varchar(40),
                    sap_accnt varchar(40),
                    local_curr_cd varchar(10),
                    curr_jj_period number(18,0),
                    prev_jj_period number(18,0),
                    jj_mnth number(18,0),
                    jj_mnth_shrt varchar(3),
                    jj_year number(18,0),
                    jj_period number(18,0),
                    jj_qrtr number(18,0),
                    to_ccy varchar(5),
                    exch_rate number(15,5),
                    gts number(38,7),
                    futr_gts number(38,9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



