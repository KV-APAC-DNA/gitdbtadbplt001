{% macro build_edw_promax_ciw_snapshot() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    pcfedw_integration.edw_promax_ciw_snapshot
                {% else %}
                    {{schema}}.pcfedw_integration__edw_promax_ciw_snapshot
                {% endif %}
        (
                		        snapshot_date timestamp_ntz(9),
                                snapshot_month varchar(10),
                                snapshot_year number(18,0),
                                jj_period number(18,0),
                                jj_wk varchar(1),
                                jj_mnth number(18,0),
                                jj_mnth_shrt varchar(3),
                                jj_mnth_long varchar(10),
                                jj_qrtr number(18,0),
                                jj_year number(18,0),
                                cust_no varchar(10),
                                cmp_desc varchar(20),
                                channel_desc varchar(20),
                                cust_nm varchar(100),
                                sales_office_desc varchar(30),
                                sales_grp_desc varchar(30),
                                matl_id varchar(40),
                                matl_desc varchar(100),
                                parent_matl_id varchar(18),
                                parent_matl_desc varchar(100),
                                fran_desc varchar(100),
                                grp_fran_desc varchar(100),
                                matl_type_desc varchar(40),
                                prod_fran_desc varchar(100),
                                prod_mjr_desc varchar(100),
                                prod_mnr_desc varchar(100),
                                base_curr_cd varchar(10),
                                to_ccy varchar(5),
                                px_qty number(38,0),
                                px_gts number(38,7),
                                px_eff_val float,
                                px_jgf_si_val float,
                                px_pmt_terms_val float,
                                px_datains_val float,
                                px_exp_adj_val float,
                                px_jgf_sd_val float,
                                px_ciw_tot float,
                                px_nts number(38,7)
        ); 
        create or replace table {{this}} clone {% if target.name=='prod' %}
                    pcfedw_integration.edw_promax_ciw_snapshot
                {% else %}
                    {{schema}}.pcfedw_integration__edw_promax_ciw_snapshot
                {% endif %};
        delete from {{this}} where to_date(snapshot_date) < (current_date -100)                              
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



