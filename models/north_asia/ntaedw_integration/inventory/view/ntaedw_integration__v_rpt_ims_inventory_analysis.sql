WITH edw_ims_fact
AS (
    SELECT *
    FROM {{ref('ntaedw_integration__edw_ims_fact')}}
    ),
v_intrm_calendar_ims
AS (
    SELECT *
    FROM {{ref('ntaedw_integration__v_intrm_calendar_ims')}}
    ),
edw_product_attr_dim
AS (
    SELECT *
    FROM {{ref('aspedw_integration__edw_product_attr_dim')}}
    ),
edw_customer_attr_flat_dim
AS (
    SELECT *
    FROM {{ref('aspedw_integration__edw_customer_attr_flat_dim')}}
    ),
itg_ims_dstr_cust_attr
AS (
    SELECT *
    FROM {{ ref('ntaitg_integration__itg_ims_dstr_cust_attr') }}
    ),
v_intrm_crncy_exch
AS (
    SELECT *
    FROM {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
    ),
t1
AS (
    SELECT ims_txn_dt,
        dstr_cd,
        dstr_nm,
        cust_cd,
        cust_nm,
        prod_cd,
        prod_nm,
        rpt_per_strt_dt,
        rpt_per_end_dt,
        ean_num,
        uom,
        unit_prc,
        sls_amt,
        sls_qty,
        rtrn_qty,
        rtrn_amt,
        ship_cust_nm,
        cust_cls_grp,
        cust_sub_cls,
        prod_spec,
        itm_agn_nm,
        ordr_co,
        rtrn_rsn,
        sls_ofc_cd,
        sls_grp_cd,
        sls_ofc_nm,
        sls_grp_nm,
        acc_type,
        co_cd,
        sls_rep_cd,
        sls_rep_nm,
        doc_dt,
        doc_num,
        invc_num,
        remark_desc,
        gift_qty,
        sls_bfr_tax_amt,
        sku_per_box,
        ctry_cd,
        crncy_cd,
        crt_dttm,
        updt_dttm
    FROM edw_ims_fact
    WHERE (
            (
                "date_part" (
                    'year',
                    ims_txn_dt::DATE
                    ) > (
                    "date_part" (
                        'year',
                        SYSDATE()::DATE
                        ) - 6
                    )
                )
            AND ((ctry_cd)::TEXT <> ('HK'::CHARACTER VARYING)::TEXT)
            )
    ),
f
AS (
    SELECT DISTINCT (ean)::CHARACTER VARYING(100) AS ean_num,
        cntry,
        sap_matl_num,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        lcl_prod_nm
    FROM edw_product_attr_dim
    ),
h
AS (
    SELECT sold_to_party,
        "max" ((sls_grp)::TEXT) AS sls_grp,
        "max" ((store_typ)::TEXT) AS store_typ
    FROM edw_customer_attr_flat_dim
    GROUP BY sold_to_party
    ),
"k"
AS (
    SELECT DISTINCT itg_ims_dstr_cust_attr.dstr_cd,
        "replace" (
            "replace" (
                "replace" (
                    "replace" (
                        (itg_ims_dstr_cust_attr.dstr_cust_cd)::TEXT,
                        ('Indirect'::CHARACTER VARYING)::TEXT,
                        (''::CHARACTER VARYING)::TEXT
                        ),
                    ('Direct'::CHARACTER VARYING)::TEXT,
                    (''::CHARACTER VARYING)::TEXT
                    ),
                ('Indi'::CHARACTER VARYING)::TEXT,
                (''::CHARACTER VARYING)::TEXT
                ),
            ('Dire'::CHARACTER VARYING)::TEXT,
            (''::CHARACTER VARYING)::TEXT
            ) AS dstr_cust_cd,
        itg_ims_dstr_cust_attr.dstr_cust_clsn1,
        itg_ims_dstr_cust_attr.dstr_cust_clsn2
    FROM itg_ims_dstr_cust_attr
    ),
a_b
AS (
    SELECT a.*,
        b.fisc_per,
        b.cal_wk,
        b.no_of_wks,
        b.fisc_wk_num
    FROM t1 a
    LEFT JOIN v_intrm_calendar_ims b ON b.cal_day = a.ims_txn_dt
    ),
a_f
AS (
    SELECT a.*,
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
        f.sap_matl_num
    FROM a_b a
    LEFT JOIN f ON ((f.ean_num)::TEXT = (a.ean_num)::TEXT)
        AND ((f.cntry)::TEXT = (a.ctry_cd)::TEXT)
    ),
a_h
AS (
    SELECT a.*,
        h.sls_grp,
        h.store_typ
    FROM a_f a
    LEFT JOIN h ON (((a.dstr_cd)::TEXT = (h.sold_to_party)::TEXT))
    ),
a_k
AS (
    SELECT a.*,
        "k".dstr_cust_clsn1,
        "k".dstr_cust_clsn2
    FROM a_h a
    LEFT JOIN "k" ON (("k".dstr_cd)::TEXT = (a.dstr_cd)::TEXT)
        AND ("k".dstr_cust_cd = (a.cust_cd)::TEXT)
    ),
a_g
AS (
    SELECT a.*,
        g.to_crncy,
        g.ex_rt
    FROM a_k a
    LEFT JOIN v_intrm_crncy_exch g ON (((a.crncy_cd)::TEXT = (g.from_crncy)::TEXT))
    ),
a_final
AS (
    SELECT dstr_cd,
        ((((dstr_cd)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (dstr_nm)::TEXT))::CHARACTER VARYING AS dstr_nm,
        cust_cd,
        cust_nm,
        COALESCE(dstr_cust_clsn1, 'Not \012Available'::CHARACTER VARYING) AS channel1,
        COALESCE(dstr_cust_clsn2, 'Not Available'::CHARACTER VARYING) AS channel2,
        ims_txn_dt,
        fisc_per,
        cal_wk AS fisc_wk,
        no_of_wks,
        fisc_wk_num,
        prod_cd,
        prod_nm,
        ean_num,
        sum(sls_amt) AS sls_amt,
        sum(sls_qty) AS sls_qty,
        sum(rtrn_qty) AS rtrn_qty,
        sum(rtrn_amt) AS rtrn_amt,
        COALESCE(COALESCE(sls_rep_nm, sls_rep_cd), 'Not Available'::CHARACTER VARYING) AS sls_rep_nm,
        sls_rep_cd,
        CASE 
            WHEN ((ctry_cd)::TEXT = ('KR'::CHARACTER VARYING)::TEXT)
                THEN 'South Korea'::CHARACTER VARYING
            WHEN ((ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
                THEN 'Taiwan'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS ctry_nm,
        crncy_cd AS from_crncy,
        to_crncy,
        ex_rt,
        CASE 
            WHEN (
                    ((ctry_cd)::TEXT = ('TW'::CHARACTER VARYING)::TEXT)
                    AND (
                        (prod_hier_l1 IS NULL)
                        OR ((prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
                        )
                    )
                THEN 'Taiwan'::CHARACTER VARYING
            WHEN (
                    ((ctry_cd)::TEXT = ('KR'::CHARACTER VARYING)::TEXT)
                    AND (
                        (prod_hier_l1 IS NULL)
                        OR ((prod_hier_l1)::TEXT = (''::CHARACTER VARYING)::TEXT)
                        )
                    )
                THEN 'Korea'::CHARACTER VARYING
            ELSE COALESCE(prod_hier_l1, '#'::CHARACTER VARYING)
            END AS prod_hier_l1,
        COALESCE(prod_hier_l2, '#'::CHARACTER VARYING) AS prod_hier_l2,
        COALESCE(prod_hier_l3, '#'::CHARACTER VARYING) AS prod_hier_l3,
        COALESCE(prod_hier_l4, '#'::CHARACTER VARYING) AS prod_hier_l4,
        COALESCE(prod_hier_l5, '#'::CHARACTER VARYING) AS prod_hier_l5,
        COALESCE(prod_hier_l6, '#'::CHARACTER VARYING) AS prod_hier_l6,
        COALESCE(prod_hier_l7, '#'::CHARACTER VARYING) AS prod_hier_l7,
        COALESCE(prod_hier_l8, '#'::CHARACTER VARYING) AS prod_hier_l8,
        COALESCE(prod_hier_l9, '#'::CHARACTER VARYING) AS prod_hier_l9,
        sap_matl_num,
        COALESCE(lcl_prod_nm, '#'::CHARACTER VARYING) AS lcl_prod_nm,
        (COALESCE(sls_grp, ('Not Available'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS sls_grp,
        (
            CASE 
                WHEN (
                        (store_typ IS NULL)
                        OR (((store_typ)::CHARACTER VARYING)::TEXT = (''::CHARACTER VARYING)::TEXT)
                        )
                    THEN ('Not Available'::CHARACTER VARYING)::TEXT
                ELSE store_typ
                END
            )::CHARACTER VARYING AS store_typ,
        CASE 
            WHEN (
                    (
                        (
                            (
                                ((prod_cd)::TEXT LIKE ('1U%'::CHARACTER VARYING)::TEXT)
                                OR ((prod_cd)::TEXT LIKE ('COUNTER TOP%'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((prod_cd)::TEXT LIKE ('DUMPBIN%'::CHARACTER VARYING)::TEXT)
                            )
                        OR (prod_cd IS NULL)
                        )
                    OR ((prod_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    )
                THEN 'non sellable products'::CHARACTER VARYING
            ELSE 'sellable products'::CHARACTER VARYING
            END AS non_sellable_product
    FROM a_g
    GROUP BY dstr_cd,
        dstr_nm,
        cust_cd,
        cust_nm,
        dstr_cust_clsn1,
        dstr_cust_clsn2,
        ims_txn_dt,
        fisc_per,
        cal_wk,
        no_of_wks,
        fisc_wk_num,
        prod_cd,
        prod_nm,
        ean_num,
        sls_rep_cd,
        sls_rep_nm,
        ctry_cd,
        crncy_cd,
        to_crncy,
        ex_rt,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        lcl_prod_nm,
        sap_matl_num,
        sls_grp,
        store_typ
    ),
s1
AS (
    SELECT edw_ims_fact.ims_txn_dt,
        edw_ims_fact.dstr_cd,
        edw_ims_fact.dstr_nm,
        edw_ims_fact.cust_cd,
        cust_cd_adj.cust_nm,
        edw_ims_fact.prod_cd,
        edw_ims_fact.prod_nm,
        edw_ims_fact.rpt_per_strt_dt,
        edw_ims_fact.rpt_per_end_dt,
        edw_ims_fact.ean_num,
        edw_ims_fact.uom,
        edw_ims_fact.unit_prc,
        edw_ims_fact.sls_amt,
        edw_ims_fact.sls_qty,
        edw_ims_fact.rtrn_qty,
        edw_ims_fact.rtrn_amt,
        edw_ims_fact.ship_cust_nm,
        edw_ims_fact.cust_cls_grp,
        edw_ims_fact.cust_sub_cls,
        edw_ims_fact.prod_spec,
        edw_ims_fact.itm_agn_nm,
        edw_ims_fact.ordr_co,
        edw_ims_fact.rtrn_rsn,
        edw_ims_fact.sls_ofc_cd,
        edw_ims_fact.sls_grp_cd,
        edw_ims_fact.sls_ofc_nm,
        edw_ims_fact.sls_grp_nm,
        edw_ims_fact.acc_type,
        edw_ims_fact.co_cd,
        edw_ims_fact.sls_rep_cd,
        edw_ims_fact.sls_rep_nm,
        edw_ims_fact.doc_dt,
        edw_ims_fact.doc_num,
        edw_ims_fact.invc_num,
        edw_ims_fact.remark_desc,
        edw_ims_fact.gift_qty,
        edw_ims_fact.sls_bfr_tax_amt,
        edw_ims_fact.sku_per_box,
        edw_ims_fact.ctry_cd,
        edw_ims_fact.crncy_cd,
        edw_ims_fact.crt_dttm,
        edw_ims_fact.updt_dttm
    FROM (
        edw_ims_fact edw_ims_fact LEFT JOIN (
            SELECT derived_table2.cust_cd,
                derived_table2.cust_nm
            FROM (
                SELECT derived_table1.cust_cd,
                    derived_table1.cust_nm,
                    derived_table1.ims_txn_dt,
                    rank() OVER (
                        PARTITION BY derived_table1.cust_cd ORDER BY derived_table1.ims_txn_dt DESC
                        ) AS rnk
                FROM (
                    SELECT DISTINCT edw_ims_fact.ims_txn_dt,
                        edw_ims_fact.cust_cd,
                        edw_ims_fact.cust_nm
                    FROM edw_ims_fact
                    WHERE ((edw_ims_fact.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
                    ) derived_table1
                ORDER BY derived_table1.cust_cd,
                    rank() OVER (
                        PARTITION BY derived_table1.cust_cd ORDER BY derived_table1.ims_txn_dt DESC
                        )
                ) derived_table2
            WHERE (derived_table2.rnk = 1)
            ) cust_cd_adj ON (((edw_ims_fact.cust_cd)::TEXT = (cust_cd_adj.cust_cd)::TEXT))
        )
    WHERE (
            (
                "date_part" (
                    'year',
                    edw_ims_fact.ims_txn_dt::DATE
                    )::VARCHAR > (
                    "date_part" (
                        'year',
                        SYSDATE()::DATE
                        ) - 6
                    )::VARCHAR
                )
            AND ((edw_ims_fact.ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
            )
    ),
s2
AS (
    SELECT a.*,
        b.fisc_per,
        b.cal_wk,
        b.no_of_wks,
        b.fisc_wk_num
    FROM s1 a
    LEFT JOIN v_intrm_calendar_ims b ON ((b.cal_day = a.ims_txn_dt))
    ),
s3
AS (
    SELECT a.*,
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
        f.sap_matl_num
    FROM s2 a
    LEFT JOIN f ON (
            (
                ((f.ean_num)::TEXT = (a.ean_num)::TEXT)
                AND ((f.cntry)::TEXT = (a.ctry_cd)::TEXT)
                )
            )
    ),
s4
AS (
    SELECT a.*,
        h.sls_grp,
        h.store_typ
    FROM s3 a
    LEFT JOIN h ON (((a.dstr_cd)::TEXT = (h.sold_to_party)::TEXT))
    ),
s5
AS (
    SELECT a.*,
        "k".dstr_cust_clsn1,
        "k".dstr_cust_clsn2
    FROM s4 a
    LEFT JOIN "k" ON (
            (
                (("k".dstr_cd)::TEXT = (a.dstr_cd)::TEXT)
                AND ("k".dstr_cust_cd = (a.cust_cd)::TEXT)
                )
            )
    ),
s6
AS (
    SELECT a.*,
        g.to_crncy,
        g.ex_rt
    FROM s5 a
    LEFT JOIN v_intrm_crncy_exch g ON (((a.crncy_cd)::TEXT = (g.from_crncy)::TEXT))
    ),
b_final
AS (
    SELECT dstr_cd,
        ((((dstr_cd)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (dstr_nm)::TEXT))::CHARACTER VARYING AS dstr_nm,
        cust_cd,
        cust_nm,
        COALESCE(dstr_cust_clsn1, 'Not \012Available'::CHARACTER VARYING) AS channel1,
        COALESCE(dstr_cust_clsn2, 'Not Available'::CHARACTER VARYING) AS channel2,
        ims_txn_dt,
        fisc_per,
        cal_wk AS fisc_wk,
        no_of_wks,
        fisc_wk_num,
        prod_cd,
        prod_nm,
        ean_num,
        sum(sls_amt) AS sls_amt,
        sum(sls_qty) AS sls_qty,
        sum(rtrn_qty) AS rtrn_qty,
        sum(rtrn_amt) AS rtrn_amt,
        COALESCE(COALESCE(sls_rep_nm, sls_rep_cd), 'Not Available'::CHARACTER VARYING) AS sls_rep_nm,
        sls_rep_cd,
        CASE 
            WHEN ((ctry_cd)::TEXT = ('HK'::CHARACTER VARYING)::TEXT)
                THEN 'Hong Kong'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS ctry_nm,
        crncy_cd AS from_crncy,
        to_crncy,
        ex_rt,
        CASE 
            WHEN (
            ((ctry_cd)::text = ('HK'::character varying)::text)
            AND (
                (prod_hier_l1 IS NULL)
                OR ((prod_hier_l1)::text = (''::character varying)::text)
                )
            ) 
            THEN 'HK'::character varying
            ELSE NULL::character varying
        END AS prod_hier_l1,
        COALESCE(prod_hier_l2, '#'::CHARACTER VARYING) AS prod_hier_l2,
        COALESCE(prod_hier_l3, '#'::CHARACTER VARYING) AS prod_hier_l3,
        COALESCE(prod_hier_l4, '#'::CHARACTER VARYING) AS prod_hier_l4,
        COALESCE(prod_hier_l5, '#'::CHARACTER VARYING) AS prod_hier_l5,
        COALESCE(prod_hier_l6, '#'::CHARACTER VARYING) AS prod_hier_l6,
        COALESCE(prod_hier_l7, '#'::CHARACTER VARYING) AS prod_hier_l7,
        COALESCE(prod_hier_l8, '#'::CHARACTER VARYING) AS prod_hier_l8,
        COALESCE(prod_hier_l9, '#'::CHARACTER VARYING) AS prod_hier_l9,
        sap_matl_num,
        COALESCE(lcl_prod_nm, '#'::CHARACTER VARYING) AS lcl_prod_nm,
        (COALESCE(sls_grp, ('Not Available'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS sls_grp,
        (
            CASE 
                WHEN (
                        (store_typ IS NULL)
                        OR (((store_typ)::CHARACTER VARYING)::TEXT = (''::CHARACTER VARYING)::TEXT)
                        )
                    THEN ('Not Available'::CHARACTER VARYING)::TEXT
                ELSE store_typ
                END
            )::CHARACTER VARYING AS store_typ,
        CASE 
            WHEN (
                    (
                        (
                            (
                                ((prod_cd)::TEXT LIKE ('1U%'::CHARACTER VARYING)::TEXT)
                                OR ((prod_cd)::TEXT LIKE ('COUNTER TOP%'::CHARACTER VARYING)::TEXT)
                                )
                            OR ((prod_cd)::TEXT LIKE ('DUMPBIN%'::CHARACTER VARYING)::TEXT)
                            )
                        OR (prod_cd IS NULL)
                        )
                    OR ((prod_cd)::TEXT = (''::CHARACTER VARYING)::TEXT)
                    )
                THEN 'non sellable products'::CHARACTER VARYING
            ELSE 'sellable products'::CHARACTER VARYING
            END AS non_sellable_product
    FROM s6
    GROUP BY dstr_cd,
        dstr_nm,
        cust_cd,
        cust_nm,
        dstr_cust_clsn1,
        dstr_cust_clsn2,
        ims_txn_dt,
        fisc_per,
        cal_wk,
        no_of_wks,
        fisc_wk_num,
        prod_cd,
        prod_nm,
        ean_num,
        sls_rep_cd,
        sls_rep_nm,
        ctry_cd,
        crncy_cd,
        to_crncy,
        ex_rt,
        prod_hier_l1,
        prod_hier_l2,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        prod_hier_l7,
        prod_hier_l8,
        prod_hier_l9,
        lcl_prod_nm,
        sap_matl_num,
        sls_grp,
        store_typ
    ),
final
AS (
    SELECT *
    FROM a_final
    
    UNION ALL
    
    SELECT *
    FROM b_final
    )
SELECT *
FROM final
