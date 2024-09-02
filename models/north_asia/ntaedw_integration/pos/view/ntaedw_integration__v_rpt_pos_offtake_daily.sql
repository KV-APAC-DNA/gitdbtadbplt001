{{
    config(
        materialized='view'
    )
}}

with edw_pos_fact as
(
    select * from {{ ref('ntaedw_integration__edw_pos_fact') }}
),
edw_customer_attr_flat_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
v_interm_cust_hier_dim as
(
    select * from {{ ref('ntaedw_integration__v_interm_cust_hier_dim') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
v_calendar_promo_dtls as
(
    select * from {{ ref('aspedw_integration__v_calendar_promo_dtls') }}
),
v_intrm_crncy_exch as 
(
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
),
final as 
(
    SELECT 
    a.pos_dt, 
    b.fisc_per, 
    b.fisc_wk, 
    c.fisc_wk_strt_dt, 
    c.fisc_wk_end_dt, 
    b.promo_per, 
    b.promo_wk, 
    h.promo_wk_strt_dt, 
    h.promo_wk_end_dt, 
    b.univ_per, 
    b.univ_wk, 
    c.fisc_wk_strt_dt AS univ_wk_strt_dt, 
    c.fisc_wk_end_dt AS univ_wk_end_dt, 
    sum(a.sls_qty) AS sls_qty, 
    CASE WHEN (
        (a.ctry_cd):: text = ('TW' :: character varying):: text
    ) THEN sum(a.prom_sls_amt) ELSE sum(a.sls_amt) END AS sls_amt, 
    a.str_cd, 
    a.str_nm, 
    a.crncy_cd, 
    a.src_sys_cd, 
    CASE WHEN (
        (a.ctry_cd):: text = ('KR' :: character varying):: text
    ) THEN 'South Korea' :: character varying WHEN (
        (a.ctry_cd):: text = ('TW' :: character varying):: text
    ) THEN 'Taiwan' :: character varying WHEN (
        (a.ctry_cd):: text = ('HK' :: character varying):: text
    ) THEN 'Hong Kong' :: character varying ELSE NULL :: character varying END AS ctry_nm, 
    COALESCE(
        a.sls_grp, 'Not Available' :: character varying
    ) AS sls_grp, 
    COALESCE(
        a.mysls_brnd_nm, 'Not Available' :: character varying
    ) AS my_sls_brand_nm, 
    COALESCE(
        a.mysls_catg, 'Not Available' :: character varying
    ) AS mysls_catg, 
    a.vend_prod_cd, 
    COALESCE(
        a.matl_num, 'Not Available' :: character varying
    ) AS matl_num, 
    COALESCE(
        a.matl_desc, 'Not Available' :: character varying
    ) AS matl_desc, 
    a.ean_num, 
    g.to_crncy, 
    g.ex_rt_typ, 
    g.ex_rt, 
    CASE WHEN (
        (
        (a.ctry_cd):: text = ('TW' :: character varying):: text
        ) 
        OR (
        (a.ctry_cd):: text = ('HK' :: character varying):: text
        )
    ) THEN COALESCE(
        d.channel, 'Not Available' :: character varying
    ) ELSE COALESCE(
        i.channel, 'Not Available' :: character varying
    ) END AS channel, 
    COALESCE(
        e.cust_hier_l1, '#' :: character varying
    ) AS cust_hier_l1, 
    COALESCE(
        e.cust_hier_l2, '#' :: character varying
    ) AS cust_hier_l2, 
    COALESCE(
        e.cust_hier_l3, '#' :: character varying
    ) AS cust_hier_l3, 
    COALESCE(
        e.cust_hier_l4, '#' :: character varying
    ) AS cust_hier_l4, 
    COALESCE(
        e.cust_hier_l5, '#' :: character varying
    ) AS cust_hier_l5, 
    CASE WHEN (
        (
        (a.ctry_cd):: text = ('TW' :: character varying):: text
        ) 
        AND (
        (f.prod_hier_l1 IS NULL) 
        OR (
            (f.prod_hier_l1):: text = ('' :: character varying):: text
        )
        )
    ) THEN 'Taiwan' :: character varying WHEN (
        (
        (a.ctry_cd):: text = ('KR' :: character varying):: text
        ) 
        AND (
        (f.prod_hier_l1 IS NULL) 
        OR (
            (f.prod_hier_l1):: text = ('' :: character varying):: text
        )
        )
    ) THEN 'Korea' :: character varying WHEN (
        (
        (a.ctry_cd):: text = ('HK' :: character varying):: text
        ) 
        AND (
        (f.prod_hier_l1 IS NULL) 
        OR (
            (f.prod_hier_l1):: text = ('' :: character varying):: text
        )
        )
    ) THEN 'HK' :: character varying ELSE COALESCE(
        f.prod_hier_l1, '#' :: character varying
    ) END AS prod_hier_l1, 
    COALESCE(
        f.prod_hier_l2, '#' :: character varying
    ) AS prod_hier_l2, 
    COALESCE(
        f.prod_hier_l3, '#' :: character varying
    ) AS prod_hier_l3, 
    COALESCE(
        f.prod_hier_l4, '#' :: character varying
    ) AS prod_hier_l4, 
    COALESCE(
        f.prod_hier_l5, '#' :: character varying
    ) AS prod_hier_l5, 
    COALESCE(
        f.prod_hier_l6, '#' :: character varying
    ) AS prod_hier_l6, 
    COALESCE(
        f.prod_hier_l7, '#' :: character varying
    ) AS prod_hier_l7, 
    COALESCE(
        f.prod_hier_l8, '#' :: character varying
    ) AS prod_hier_l8, 
    COALESCE(
        f.prod_hier_l9, '#' :: character varying
    ) AS prod_hier_l9, 
    COALESCE(
        a.prom_prc_amt, 
        (
        (0):: numeric
        ):: numeric(18, 0)
    ) AS price, 
    COALESCE(
        f.lcl_prod_nm, '#' :: character varying
    ) AS lcl_prod_nm 
    FROM 
    (
        (
        (
            (
            (
                (
                (
                    (
                    (
                        SELECT 
                        edw_pos_fact.pos_dt, 
                        edw_pos_fact.vend_cd, 
                        edw_pos_fact.vend_nm, 
                        edw_pos_fact.prod_nm, 
                        edw_pos_fact.vend_prod_cd, 
                        edw_pos_fact.vend_prod_nm, 
                        edw_pos_fact.brnd_nm, 
                        edw_pos_fact.ean_num, 
                        edw_pos_fact.str_cd, 
                        edw_pos_fact.str_nm, 
                        edw_pos_fact.sls_qty, 
                        edw_pos_fact.sls_amt, 
                        edw_pos_fact.unit_prc_amt, 
                        edw_pos_fact.sls_excl_vat_amt, 
                        edw_pos_fact.stk_rtrn_amt, 
                        edw_pos_fact.stk_recv_amt, 
                        edw_pos_fact.avg_sell_qty, 
                        edw_pos_fact.cum_ship_qty, 
                        edw_pos_fact.cum_rtrn_qty, 
                        edw_pos_fact.web_ordr_takn_qty, 
                        edw_pos_fact.web_ordr_acpt_qty, 
                        edw_pos_fact.dc_invnt_qty, 
                        edw_pos_fact.invnt_qty, 
                        edw_pos_fact.invnt_amt, 
                        edw_pos_fact.invnt_dt, 
                        edw_pos_fact.serial_num, 
                        edw_pos_fact.prod_delv_type, 
                        edw_pos_fact.prod_type, 
                        edw_pos_fact.dept_cd, 
                        edw_pos_fact.dept_nm, 
                        edw_pos_fact.spec_1_desc, 
                        edw_pos_fact.spec_2_desc, 
                        edw_pos_fact.cat_big, 
                        edw_pos_fact.cat_mid, 
                        edw_pos_fact.cat_small, 
                        edw_pos_fact.dc_prod_cd, 
                        edw_pos_fact.cust_dtls, 
                        edw_pos_fact.dist_cd, 
                        edw_pos_fact.crncy_cd, 
                        edw_pos_fact.src_txn_sts, 
                        edw_pos_fact.src_seq_num, 
                        edw_pos_fact.src_sys_cd, 
                        edw_pos_fact.ctry_cd, 
                        edw_pos_fact.sold_to_party, 
                        edw_pos_fact.sls_grp, 
                        edw_pos_fact.mysls_brnd_nm, 
                        edw_pos_fact.mysls_catg, 
                        edw_pos_fact.matl_num, 
                        edw_pos_fact.matl_desc, 
                        edw_pos_fact.hist_flg, 
                        edw_pos_fact.crt_dttm, 
                        edw_pos_fact.updt_dttm, 
                        edw_pos_fact.prom_sls_amt, 
                        edw_pos_fact.prom_prc_amt 
                        FROM 
                        edw_pos_fact 
                        WHERE 
                        (
                            "date_part"(
                            year, 
                            edw_pos_fact.pos_dt
                            ) > (
                            "date_part"(
                                year, 
                                (convert_timezone('UTC',current_timestamp()) :: character varying):: timestamp without time zone
                            ) -3
                            )
                        )
                    ) a 
                    LEFT JOIN (
                        SELECT 
                        DISTINCT edw_customer_attr_flat_dim.channel, 
                        edw_customer_attr_flat_dim.sold_to_party, 
                        edw_customer_attr_flat_dim.cust_store_ref 
                        FROM 
                        edw_customer_attr_flat_dim
                    ) d ON (
                        (
                        (
                            (
                            (
                                COALESCE(
                                d.sold_to_party, '~' :: character varying
                                )
                            ):: text = (
                                COALESCE(
                                a.sold_to_party, '~' :: character varying
                                )
                            ):: text
                            ) 
                            AND (
                            (
                                COALESCE(
                                d.cust_store_ref, '#' :: character varying
                                )
                            ):: text = (
                                COALESCE(a.str_cd, '~' :: character varying)
                            ):: text
                            )
                        ) 
                        AND (
                            (
                            (a.ctry_cd):: text = ('TW' :: character varying):: text
                            ) 
                            OR (
                            (a.ctry_cd):: text = ('HK' :: character varying):: text
                            )
                        )
                        )
                    )
                    ) 
                    LEFT JOIN (
                    SELECT 
                        DISTINCT edw_customer_attr_flat_dim.channel, 
                        edw_customer_attr_flat_dim.sold_to_party 
                    FROM 
                        edw_customer_attr_flat_dim
                    ) i ON (
                    (
                        (
                        (
                            COALESCE(
                            i.sold_to_party, '~' :: character varying
                            )
                        ):: text = (
                            COALESCE(
                            a.sold_to_party, '~' :: character varying
                            )
                        ):: text
                        ) 
                        AND (
                        (a.ctry_cd):: text = ('KR' :: character varying):: text
                        )
                    )
                    )
                ) 
                LEFT JOIN (
                    SELECT 
                    DISTINCT v_interm_cust_hier_dim.sold_to_party, 
                    v_interm_cust_hier_dim.cust_hier_l1, 
                    v_interm_cust_hier_dim.cust_hier_l2, 
                    v_interm_cust_hier_dim.cust_hier_l3, 
                    v_interm_cust_hier_dim.cust_hier_l4, 
                    v_interm_cust_hier_dim.cust_hier_l5 
                    FROM 
                    v_interm_cust_hier_dim
                ) e ON (
                    (
                    (
                        COALESCE(
                        e.sold_to_party, '~' :: character varying
                        )
                    ):: text = (
                        COALESCE(
                        a.sold_to_party, '~' :: character varying
                        )
                    ):: text
                    )
                )
                ) 
                LEFT JOIN (
                SELECT 
                    DISTINCT (edw_product_attr_dim.ean):: character varying(100) AS ean_num, 
                    edw_product_attr_dim.prod_hier_l1, 
                    edw_product_attr_dim.prod_hier_l2, 
                    edw_product_attr_dim.prod_hier_l3, 
                    edw_product_attr_dim.prod_hier_l4, 
                    edw_product_attr_dim.prod_hier_l5, 
                    edw_product_attr_dim.prod_hier_l6, 
                    edw_product_attr_dim.prod_hier_l7, 
                    edw_product_attr_dim.prod_hier_l8, 
                    edw_product_attr_dim.prod_hier_l9, 
                    edw_product_attr_dim.cntry, 
                    edw_product_attr_dim.lcl_prod_nm 
                FROM 
                    edw_product_attr_dim edw_product_attr_dim
                ) f ON (
                (
                    (
                    (f.ean_num):: text = (a.ean_num):: text
                    ) 
                    AND (
                    (f.cntry):: text = (a.ctry_cd):: text
                    )
                )
                )
            ) 
            LEFT JOIN (
                SELECT 
                x.cal_day, 
                x.cal_wk AS univ_wk, 
                x.cal_wk AS fisc_wk, 
                (
                    "substring"(x.promo_per, 1, 4) || (
                    CASE WHEN (
                        length(
                        (
                            (x.promo_week):: character varying(100)
                        ):: text
                        ) = 1
                    ) THEN (
                        (
                        ('0' :: character varying):: text || (
                            (x.promo_week):: character varying(100)
                        ):: text
                        )
                    ):: character varying ELSE (x.promo_week):: character varying(100) END
                    ):: text
                ) AS promo_wk, 
                x.cal_yr, 
                (
                    (
                    (x.cal_yr):: character varying
                    ):: text || CASE WHEN (
                    length(
                        (
                        (x.cal_mo_2):: character varying
                        ):: text
                    ) = 2
                    ) THEN (
                    ('0' :: character varying):: text || (
                        (x.cal_mo_2):: character varying
                    ):: text
                    ) ELSE (
                    ('00' :: character varying):: text || (
                        (x.cal_mo_2):: character varying
                    ):: text
                    ) END
                ) AS univ_per, 
                x.fisc_per, 
                x.promo_per 
                FROM 
                v_calendar_promo_dtls x
            ) b ON (
                (a.pos_dt = b.cal_day)
            )
            ) 
            LEFT JOIN (
            SELECT 
                v_calendar_promo_dtls.cal_wk AS fisc_wk, 
                min(v_calendar_promo_dtls.cal_day) AS fisc_wk_strt_dt, 
                "max"(v_calendar_promo_dtls.cal_day) AS fisc_wk_end_dt 
            FROM 
                v_calendar_promo_dtls 
            GROUP BY 
                v_calendar_promo_dtls.cal_wk
            ) c ON (
            (b.fisc_wk = c.fisc_wk)
            )
        ) 
        LEFT JOIN (
            SELECT 
            derived_table1.promo_wk, 
            min(derived_table1.cal_day) AS promo_wk_strt_dt, 
            "max"(derived_table1.cal_day) AS promo_wk_end_dt 
            FROM 
            (
                SELECT 
                v_calendar_promo_dtls.cal_day, 
                (
                    "substring"(
                    v_calendar_promo_dtls.promo_per, 
                    1, 4
                    ) || (
                    CASE WHEN (
                        length(
                        (
                            (
                            v_calendar_promo_dtls.promo_week
                            ):: character varying(100)
                        ):: text
                        ) = 1
                    ) THEN (
                        (
                        ('0' :: character varying):: text || (
                            (
                            v_calendar_promo_dtls.promo_week
                            ):: character varying(100)
                        ):: text
                        )
                    ):: character varying ELSE (
                        v_calendar_promo_dtls.promo_week
                    ):: character varying(100) END
                    ):: text
                ) AS promo_wk 
                FROM 
                v_calendar_promo_dtls
            ) derived_table1 
            GROUP BY 
            derived_table1.promo_wk
        ) h ON (
            (b.promo_wk = h.promo_wk)
        )
        ) 
        LEFT JOIN v_intrm_crncy_exch g ON (
        (
            (a.crncy_cd):: text = (g.from_crncy):: text
        )
        )
    ) 
    GROUP BY 
    a.pos_dt, 
    b.fisc_per, 
    a.crncy_cd, 
    a.src_sys_cd, 
    a.ctry_cd, 
    a.sls_grp, 
    a.mysls_brnd_nm, 
    g.to_crncy, 
    g.ex_rt_typ, 
    g.ex_rt, 
    b.promo_per, 
    b.univ_per, 
    a.str_cd, 
    a.str_nm, 
    b.fisc_wk, 
    b.univ_wk, 
    b.promo_wk, 
    a.mysls_catg, 
    a.matl_num, 
    a.matl_desc, 
    a.ean_num, 
    a.vend_prod_cd, 
    d.channel, 
    i.channel, 
    e.cust_hier_l1, 
    e.cust_hier_l2, 
    e.cust_hier_l3, 
    e.cust_hier_l4, 
    e.cust_hier_l5, 
    f.prod_hier_l1, 
    f.prod_hier_l2, 
    f.prod_hier_l3, 
    f.prod_hier_l4, 
    f.prod_hier_l5, 
    f.prod_hier_l6, 
    f.prod_hier_l7, 
    f.prod_hier_l8, 
    f.prod_hier_l9, 
    f.lcl_prod_nm, 
    c.fisc_wk_strt_dt, 
    c.fisc_wk_end_dt, 
    h.promo_wk_strt_dt, 
    h.promo_wk_end_dt, 
    a.prom_prc_amt

)
select * from final