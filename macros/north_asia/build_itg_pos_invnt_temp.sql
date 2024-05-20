{% macro build_itg_pos_invnt_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaitg_integration.itg_pos_invnt_temp
            {% else %}
                {{schema}}.ntaitg_integration__itg_pos_invnt_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pos_invnt
                {% else %}
                    {{schema}}.ntaitg_integration__itg_pos_invnt
                {% endif %}
    (           invnt_dt date,
                vend_cd varchar(40),
                vend_nm varchar(100),
                vend_prod_cd varchar(40),
                vend_prod_nm varchar(100),
                ean_num varchar(40),
                str_cd varchar(40),
                str_nm varchar(100),
                invnt_qty number(18,0),
                invnt_amt number(16,5),
                unit_prc_amt number(16,5),
                per_box_qty number(16,5),
                cust_invnt_qty number(16,5),
                box_invnt_qty number(16,5),
                wk_hold_sls number(16,5),
                wk_hold number(16,5),
                fst_recv_dt varchar(10),
                dsct_dt varchar(10),
                dc varchar(40),
                stk_cls varchar(40),
                crncy_cd varchar(10),
                src_sys_cd varchar(30),
                ctry_cd varchar(10),
                crt_dttm timestamp_ntz(9),
                upd_dttm timestamp_ntz(9)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            ntaitg_integration.itg_pos_invnt
        {% else %}
            {{schema}}.ntaitg_integration__itg_pos_invnt
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}