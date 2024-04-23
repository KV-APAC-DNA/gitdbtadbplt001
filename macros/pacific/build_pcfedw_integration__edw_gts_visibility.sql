{% macro build_pcfedw_integration__edw_gts_visibility() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    pcfedw_integration.edw_gts_visibility
                {% else %}
                    {{schema}}.pcfedw_integration__edw_gts_visibility
                {% endif %}
         (
                	subsource_type varchar(15),
                    country varchar(11),
                    snapshot_date date,
                    snapshot_date_jnj_month number(18,0),
                    jj_mnth_id number(18,0),
                    jj_mnth number(18,0),
                    jj_mnth_nm varchar(3),
                    jj_year number(18,0),
                    jj_qrtr number(18,0),
                    cust_num varchar(10),
                    matl_num varchar(18),
                    sls_doc varchar(10),
                    rqst_delv_dt date,
                    rqst_delv_dt_jnj_month number(18,0),
                    bill_num varchar(50),
                    bill_dt date,
                    bill_dt_yyyy_mm number(18,0),
                    created_on date,
                    grp_fran_desc varchar(100),
                    prod_fran_desc varchar(100),
                    prod_mjr_desc varchar(100),
                    prod_mnr_desc varchar(100),
                    matl_desc varchar(100),
                    brnd_desc varchar(100),
                    gcph_franchise varchar(30),
                    gcph_brand varchar(30),
                    gcph_subbrand varchar(100),
                    gcph_variant varchar(100),
                    gcph_needstate varchar(50),
                    gcph_category varchar(50),
                    gcph_subcategory varchar(50),
                    gcph_segment varchar(50),
                    gcph_subsegment varchar(100),
                    master_code varchar(18),
                    channel_desc varchar(20),
                    sales_office_desc varchar(30),
                    cust_nm varchar(100),
                    sales_grp_desc varchar(30),
                    local_ccy varchar(5),
                    to_ccy varchar(5),
                    exch_rate number(15,5),
                    open_orders_val number(38,9),
                    gts_landing_val number(38,9)

        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



