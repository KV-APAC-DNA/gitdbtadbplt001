{% macro build_itg_ims_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_ims_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_ims_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_ims
                {% else %}
                    {{schema}}.ntaitg_integration__itg_ims
                {% endif %}
    (           ims_txn_dt date,
                dstr_cd varchar(10),
                dstr_nm varchar(100),
                cust_cd varchar(50),
                cust_nm varchar(100),
                prod_cd varchar(20),
                prod_nm varchar(100),
                rpt_per_strt_dt date,
                rpt_per_end_dt date,
                ean_num varchar(20),
                uom varchar(10),
                unit_prc number(21,5),
                sls_amt number(21,5),
                sls_qty number(18,0),
                rtrn_qty number(18,0),
                rtrn_amt number(21,5),
                ship_cust_nm varchar(100),
                cust_cls_grp varchar(20),
                cust_sub_cls varchar(20),
                prod_spec varchar(50),
                itm_agn_nm varchar(100),
                ordr_co varchar(20),
                rtrn_rsn varchar(100),
                sls_ofc_cd varchar(10),
                sls_grp_cd varchar(10),
                sls_ofc_nm varchar(20),
                sls_grp_nm varchar(20),
                acc_type varchar(10),
                co_cd varchar(20),
                sls_rep_cd varchar(20),
                sls_rep_nm varchar(50),
                doc_dt date,
                doc_type varchar(10),
                doc_num varchar(20),
                invc_num varchar(15),
                remark_desc varchar(100),
                gift_qty number(18,0),
                sls_bfr_tax_amt number(21,5),
                sku_per_box number(21,5),
                ctry_cd varchar(2),
                crncy_cd varchar(3),
                crt_dttm timestamp_ntz(9),
                updt_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_ims
        {% else %}
            {{schema}}.ntaitg_integration__itg_ims
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}