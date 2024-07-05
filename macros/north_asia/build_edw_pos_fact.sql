{% macro build_edw_pos_fact() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                ntaedw_integration.edw_pos_fact
            {% else %}
                {{schema}}.ntaedw_integration__edw_pos_fact
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    ntaedw_integration.edw_pos_fact
                {% else %}
                    {{schema}}.ntaedw_integration__edw_pos_fact
                {% endif %}
    (           	pos_dt date,
                    vend_cd varchar(40),
                    vend_nm varchar(100),
                    prod_nm varchar(100),
                    vend_prod_cd varchar(40),
                    vend_prod_nm varchar(100),
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
                    sold_to_party varchar(100),
                    sls_grp varchar(100),
                    mysls_brnd_nm varchar(500),
                    mysls_catg varchar(100),
                    matl_num varchar(40),
                    matl_desc varchar(100),
                    hist_flg varchar(40),
                    crt_dttm timestamp_ntz(9),
                    updt_dttm timestamp_ntz(9),
                    prom_sls_amt number(16,5),
                    prom_prc_amt number(16,5),
                    channel varchar(25),
                    store_type varchar(25),
                    sls_grp_cd varchar(18)
    );
         DELETE FROM {{tablename}}
                    WHERE
                    ctry_cd = 'TW' 
                    AND hist_flg = 'N';
        INSERT INTO {{tablename}}
                        SELECT
                        pos_dt,
                        vend_cd,
                        vend_nm,
                        prod_nm,
                        vend_prod_cd,
                        vend_prod_nm,
                        brnd_nm,
                        ean_num,
                        str_cd,
                        str_nm,
                        sls_qty,
                        sls_amt,
                        unit_prc_amt,
                        sls_excl_vat_amt,
                        stk_rtrn_amt,
                        stk_recv_amt,
                        avg_sell_qty,
                        cum_ship_qty,
                        cum_rtrn_qty,
                        web_ordr_takn_qty,
                        web_ordr_acpt_qty,
                        dc_invnt_qty,
                        invnt_qty,
                        invnt_amt,
                        invnt_dt,
                        serial_num,
                        prod_delv_type,
                        prod_type,
                        dept_cd,
                        dept_nm,
                        spec_1_desc,
                        spec_2_desc,
                        cat_big,
                        cat_mid,
                        cat_small,
                        dc_prod_cd,
                        cust_dtls,
                        dist_cd,
                        crncy_cd,
                        src_txn_sts,
                        src_seq_num,
                        src_sys_cd,
                        ctry_cd,
                        sold_to_party,
                        sls_grp,
                        mysls_brnd_nm,
                        mysls_catg,
                        matl_num,
                        matl_desc,
                        hist_flg,
                        crt_dttm,
                        updt_dttm,
                        prom_sls_amt,
                        prom_prc_amt,
                        null::varchar(25) as channel,
                        null::varchar(25) as store_type,
                        null::varchar(18) as sls_grp_cd
                    FROM
                    {{ ref('ntawks_integration__tw_pos') }}
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}