{% macro build_itg_kr_sales_target_am_brand_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_kr_sales_target_am_brand_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_kr_sales_target_am_brand_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_kr_sales_target_am_brand
                {% else %}
                    {{schema}}.ntaitg_integration__itg_kr_sales_target_am_brand
                {% endif %}
    (            pos_dt date,
                vend_cd varchar(40),
                vend_nm varchar(100),
                prod_nm varchar(100),
                vend_prod_cd varchar(40),
                vend_prod_nm varchar(600),
                brnd_nm varchar(40),
                ean_num varchar(100),
                str_cd varchar(40),
                str_nm varchar(100),
                sls_qty number(18,0),
                sls_amt number(16,5),
                unit_prc_amt number(16,5),
                sls_excl_vat_amt number(16,5),
                stk_rtrn_amt number(16,5),
                stk_recv_amt number(16,5),
                avg_sell_qty number(16,5),
                cum_ship_qty number(18,0),
                cum_rtrn_qty number(18,0),
                web_ordr_takn_qty number(18,0),
                web_ordr_acpt_qty number(18,0),
                dc_invnt_qty number(18,0),
                invnt_qty number(18,0),
                invnt_amt number(16,5),
                invnt_dt date,
                serial_num varchar(40),
                prod_delv_type varchar(40),
                prod_type varchar(40),
                dept_cd varchar(40),
                dept_nm varchar(100),
                spec_1_desc varchar(100),
                spec_2_desc varchar(100),
                cat_big varchar(100),
                cat_mid varchar(40),
                cat_small varchar(40),
                dc_prod_cd varchar(40),
                cust_dtls varchar(100),
                dist_cd varchar(40),
                crncy_cd varchar(10),
                src_txn_sts varchar(40),
                src_seq_num number(18,0),
                src_sys_cd varchar(30),
                ctry_cd varchar(10),
                src_mesg_no varchar(35),
                src_mesg_code varchar(3),
                src_mesg_func_code varchar(3),
                src_mesg_date date,
                src_sale_date_form varchar(3),
                src_send_code varchar(10),
                src_send_ean_code varchar(13),
                src_send_name varchar(30),
                src_recv_qual varchar(13),
                src_recv_ean_code varchar(10),
                src_recv_name varchar(35),
                src_part_qual varchar(3),
                src_part_ean_code varchar(13),
                src_part_id varchar(10),
                src_part_name varchar(30),
                src_sender_id varchar(35),
                src_recv_date varchar(10),
                src_recv_time varchar(6),
                src_file_size number(8,0),
                src_file_path varchar(128),
                src_lega_tran varchar(1),
                src_regi_date varchar(10),
                src_line_no number(6,0),
                src_instore_code varchar(20),
                src_mnth_sale_amnt number(15,0),
                src_qty_unit varchar(3),
                src_mnth_sale_qty number(10,0),
                unit_of_pkg_sales varchar(5),
                doc_send_date date,
                unit_of_pkg_invt varchar(5),
                doc_fun varchar(6),
                doc_no varchar(40),
                doc_fun_cd varchar(6),
                buye_loc_cd varchar(40),
                vend_loc_cd varchar(40),
                provider_loc_cd varchar(40),
                comp_qty number(18,0),
                unit_of_pkg_comp varchar(5),
                order_qty number(18,0),
                unit_of_pkg_order varchar(5),
                crt_dttm timestamp_ntz(9),
                upd_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_kr_sales_target_am_brand
        {% else %}
            {{schema}}.ntaitg_integration__itg_kr_sales_target_am_brand
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}