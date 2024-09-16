{{ config(
  sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
) }}
with v_dly_planned_ims as (
    select * from {{ ref('ntaedw_integration__v_dly_planned_ims') }}
),
v_dly_ims_txn_msl as (
    select * from {{ ref('ntaedw_integration__v_dly_ims_txn_msl') }}    
),
v_intrm_calendar_ims as (
    select * from {{ ref('ntaedw_integration__v_intrm_calendar_ims') }}
),
edw_store_dim as (
    select * from {{ ref('ntaedw_integration__edw_store_dim') }}
),
edw_product_attr_dim as (
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
edw_sls_rep_dim as (
    select * from {{ ref('ntaedw_integration__edw_sls_rep_dim') }}
),
v_intrm_crncy_exch as (
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}    
),
final as
(   
    SELECT 
        COALESCE(pln.cal_day, txn.ims_txn_dt) AS ims_txn_dt,
        b.fisc_per,
        b.cal_wk AS fisc_wk,
        b.no_of_wks,
        b.fisc_wk_num,
        pln.visit_dt,
        pln.visit_jj_mnth_id,
        pln.visit_jj_wk_no,
        pln.visit_jj_wkdy_no,
        pln.visit_end_dt,
        txn.ims_txn_dt AS actl_ims_txn_dt,
        COALESCE(pln.ctry_cd, txn.ctry_cd) AS ctry_cd,
        COALESCE(pln.dstr_cd, txn.dstr_cd) AS dstr_cd,
        CASE
            WHEN (
                (
                    (pln.ctry_cd)::text = ('HK'::character varying)::text
                )
                OR (
                    (txn.ctry_cd)::text = ('HK'::character varying)::text
                )
            ) THEN sls.dstr_nm
            WHEN (
                (
                    (pln.ctry_cd)::text = ('TW'::character varying)::text
                )
                OR (
                    (txn.ctry_cd)::text = ('TW'::character varying)::text
                )
            ) THEN (
                COALESCE(txn.dstr_nm, ('#'::character varying)::text)
            )::character varying
            ELSE NULL::character varying
        END AS dstr_nm,
        trim((COALESCE(pln.sls_rep_cd, txn.sls_rep_cd))::text) AS sls_rep_cd,
        pln.sls_rep_typ,
        sls.sls_rep_nm,
        trim((COALESCE(pln.store_cd, txn.store_cd))::text) AS store_cd,
        k.store_nm,
        COALESCE(pln.store_class, txn.store_class) AS store_class,
        COALESCE(pln.crncy_cd, txn.crncy_cd) AS from_crncy_cd,
        ((g.ex_rt)::double precision * pln.dly_store_tgt) AS dly_store_tgt,
        CASE
            WHEN (
                (txn.store_sls_amt IS NOT NULL)
                AND (pln.visit_dt IS NULL)
            ) THEN 'TNP'::character varying
            WHEN (
                (txn.store_sls_amt IS NULL)
                AND (pln.visit_dt IS NOT NULL)
            ) THEN 'PNT'::character varying
            ELSE 'P&T'::character varying
        END AS dly_plan_flg,
        txn.prod_cd,
        CASE
            WHEN (
                (
                    (
                        (
                            (
                                (txn.prod_cd)::text LIKE ('1U%'::character varying)::text
                            )
                            OR (
                                (txn.prod_cd)::text LIKE ('COUNTER TOP%'::character varying)::text
                            )
                        )
                        OR (txn.prod_cd IS NULL)
                    )
                    OR (
                        (txn.prod_cd)::text = (''::character varying)::text
                    )
                )
                OR (
                    (txn.prod_cd)::text LIKE ('DUMPBIN%'::character varying)::text
                )
            ) THEN 'non sellable products'::character varying
            ELSE 'sellable products'::character varying
        END AS non_sellable_product,
        CASE
            WHEN (
                (
                    (txn.ctry_cd)::text = ('TW'::character varying)::text
                )
                AND (
                    (f.prod_hier_l1 IS NULL)
                    OR (
                        (f.prod_hier_l1)::text = (''::character varying)::text
                    )
                )
            ) THEN 'Taiwan'::character varying
            WHEN (
                (
                    (txn.ctry_cd)::text = ('KR'::character varying)::text
                )
                AND (
                    (f.prod_hier_l1 IS NULL)
                    OR (
                        (f.prod_hier_l1)::text = (''::character varying)::text
                    )
                )
            ) THEN 'Korea'::character varying
            WHEN (
                (
                    (txn.ctry_cd)::text = ('HK'::character varying)::text
                )
                AND (
                    (f.prod_hier_l1 IS NULL)
                    OR (
                        (f.prod_hier_l1)::text = (''::character varying)::text
                    )
                )
            ) THEN 'HK'::character varying
            ELSE COALESCE(f.prod_hier_l1, '#'::character varying)
        END AS prod_hier_l1,
        COALESCE(f.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
        COALESCE(f.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
        COALESCE(f.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
        COALESCE(f.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
        COALESCE(f.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
        COALESCE(f.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
        COALESCE(f.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
        COALESCE(f.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
        COALESCE(f1.prod_hier_l9, '#'::character varying) AS prnt_prod_hier_l9,
        f.sap_matl_num,
        COALESCE(f.lcl_prod_nm, '#'::character varying) AS lcl_prod_nm,
        txn.msl_flg,
        sum(txn.store_sls_amt) OVER(
            PARTITION BY b.fisc_per,
            pln.visit_dt,
            COALESCE(pln.ctry_cd, txn.ctry_cd),
            COALESCE(pln.dstr_cd, txn.dstr_cd),
            COALESCE(pln.sls_rep_cd, txn.sls_rep_cd),
            COALESCE(pln.store_cd, txn.store_cd) 
            order by null
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS period_sls_amt,
        count(1) OVER(
            PARTITION BY COALESCE(pln.cal_day, txn.ims_txn_dt),
            COALESCE(pln.ctry_cd, txn.ctry_cd),
            COALESCE(pln.dstr_cd, txn.dstr_cd),
            COALESCE(pln.sls_rep_cd, txn.sls_rep_cd),
            COALESCE(pln.store_cd, txn.store_cd),
            g.to_crncy 
            order by null
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS dly_prod_cnt,
        txn.store_sls_amt,
        txn.ean_num,
        txn.prnt_ean_num,
        g.to_crncy,
        g.ex_rt,
        (g.ex_rt * txn.sls_amt) AS sls_amt,
        txn.sls_qty,
        txn.rtrn_qty,
        txn.rtrn_amt,
        txn.hq,
        txn.store_type,
        txn.sell_in_price_manual,
        txn.sell_out_unit_price
    FROM (
            (
                (
                    (
                        (
                            (
                                (
                                    v_dly_planned_ims pln
                                    FULL JOIN v_dly_ims_txn_msl txn ON 
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            ((pln.ctry_cd)::text = (txn.ctry_cd)::text)
                                                            AND (rtrim(pln.dstr_cd)::text = rtrim(txn.dstr_cd)::text)
                                                        )
                                                        AND (rtrim(pln.store_cd)::text = rtrim(txn.store_cd)::text)
                                                    )
                                                    AND (rtrim(pln.sls_rep_cd)::text = rtrim(txn.sls_rep_cd)::text)
                                                )
                                                AND (pln.cal_day = txn.ims_txn_dt)
                                            )
                                            AND ((pln.crncy_cd)::text = (txn.crncy_cd)::text)
                                        )
                                    )
                                )
                                LEFT JOIN v_intrm_calendar_ims b ON (
                                    (
                                        b.cal_day = COALESCE(pln.cal_day, txn.ims_txn_dt)
                                    )
                                )
                            )
                            LEFT JOIN edw_store_dim k ON (
                                (
                                    (
                                        (
                                            rtrim(k.dstr_cd)::text = rtrim(COALESCE(pln.dstr_cd, txn.dstr_cd))::text
                                        )
                                        AND (
                                            rtrim(k.store_cd)::text = rtrim(COALESCE(pln.store_cd, txn.store_cd))::text
                                        )
                                    )
                                    AND (
                                        (k.ctry_cd)::text = (COALESCE(pln.ctry_cd, txn.ctry_cd))::text
                                    )
                                )
                            )
                        )
                        LEFT JOIN 
                        (
                            SELECT DISTINCT (
                                edw_product_attr_dim.ean)::character varying(100) AS ean_num,
                                edw_product_attr_dim.cntry,
                                edw_product_attr_dim.sap_matl_num,
                                edw_product_attr_dim.prod_hier_l1,
                                edw_product_attr_dim.prod_hier_l2,
                                edw_product_attr_dim.prod_hier_l3,
                                edw_product_attr_dim.prod_hier_l4,
                                edw_product_attr_dim.prod_hier_l5,
                                edw_product_attr_dim.prod_hier_l6,
                                edw_product_attr_dim.prod_hier_l7,
                                edw_product_attr_dim.prod_hier_l8,
                                edw_product_attr_dim.prod_hier_l9,
                                edw_product_attr_dim.lcl_prod_nm
                            FROM edw_product_attr_dim
                        ) f ON (
                            (
                                CASE
                                    WHEN (
                                        (
                                            (
                                                (txn.ctry_cd)::text = ('HK'::character varying)::text
                                            )
                                            AND (rtrim(f.ean_num)::text = rtrim(txn.ean_num)::text)
                                        )
                                        AND ((f.cntry)::text = (txn.ctry_cd)::text)
                                    ) THEN 1
                                    WHEN (
                                        (
                                            (
                                                (txn.ctry_cd)::text = ('TW'::character varying)::text
                                            )
                                            AND (
                                                rtrim(f.ean_num)::text = rtrim(ltrim(
                                                    (txn.ean_num)::text,
                                                    ('0'::character varying)::text
                                                ))
                                            )
                                        )
                                        AND ((f.cntry)::text = (txn.ctry_cd)::text)
                                    ) THEN 1
                                    ELSE 0
                                END = 1
                            )
                        )
                    )
                    LEFT JOIN (
                        SELECT DISTINCT (edw_product_attr_dim.ean)::character varying(100) AS ean_num,
                            edw_product_attr_dim.cntry,
                            edw_product_attr_dim.sap_matl_num,
                            edw_product_attr_dim.prod_hier_l1,
                            edw_product_attr_dim.prod_hier_l2,
                            edw_product_attr_dim.prod_hier_l3,
                            edw_product_attr_dim.prod_hier_l4,
                            edw_product_attr_dim.prod_hier_l5,
                            edw_product_attr_dim.prod_hier_l6,
                            edw_product_attr_dim.prod_hier_l7,
                            edw_product_attr_dim.prod_hier_l8,
                            edw_product_attr_dim.prod_hier_l9,
                            edw_product_attr_dim.lcl_prod_nm
                        FROM edw_product_attr_dim
                    ) f1 ON (
                        (
                            (rtrim(f1.ean_num)::text = rtrim(txn.prnt_ean_num))
                            AND ((f1.cntry)::text = (txn.ctry_cd)::text)
                        )
                    )
                )
                LEFT JOIN edw_sls_rep_dim sls ON (
                    (
                        (
                            (
                                (COALESCE(pln.ctry_cd, txn.ctry_cd))::text = (sls.ctry_cd)::text
                            )
                            AND (
                                rtrim(COALESCE(pln.dstr_cd, txn.dstr_cd))::text = rtrim(sls.dstr_cd)::text
                            )
                        )
                        AND (
                            rtrim(COALESCE(pln.sls_rep_cd, txn.sls_rep_cd))::text = rtrim(sls.sls_rep_cd)::text
                        )
                    )
                )
            )
            LEFT JOIN v_intrm_crncy_exch g ON (
                (
                    (COALESCE(pln.crncy_cd, txn.crncy_cd))::text = (g.from_crncy)::text
                )
            )
        )
    GROUP BY pln.cal_day,
        txn.ims_txn_dt,
        b.fisc_per,
        b.cal_wk,
        b.no_of_wks,
        b.fisc_wk_num,
        pln.visit_dt,
        pln.visit_jj_mnth_id,
        pln.visit_jj_wk_no,
        pln.visit_jj_wkdy_no,
        pln.visit_end_dt,
        pln.ctry_cd,
        txn.ctry_cd,
        pln.dstr_cd,
        txn.dstr_cd,
        sls.dstr_nm,
        pln.sls_rep_cd,
        txn.sls_rep_cd,
        pln.sls_rep_typ,
        sls.sls_rep_nm,
        pln.store_cd,
        txn.store_cd,
        k.store_nm,
        pln.store_class,
        txn.store_class,
        pln.crncy_cd,
        txn.crncy_cd,
        pln.dly_store_tgt,
        CASE
            WHEN (
                (txn.store_sls_amt IS NOT NULL)
                AND (pln.visit_dt IS NULL)
            ) THEN 'TNP'::character varying
            WHEN (
                (txn.store_sls_amt IS NULL)
                AND (pln.visit_dt IS NOT NULL)
            ) THEN 'PNT'::character varying
            ELSE 'P&T'::character varying
        END,
        txn.prod_cd,
        f.prod_hier_l1,
        f.prod_hier_l2,
        f.prod_hier_l3,
        f.prod_hier_l4,
        f.prod_hier_l5,
        f.prod_hier_l6,
        f.prod_hier_l7,
        f.prod_hier_l8,
        f.prod_hier_l9,
        f1.prod_hier_l9,
        f.sap_matl_num,
        f.lcl_prod_nm,
        txn.msl_flg,
        txn.store_sls_amt,
        txn.ean_num,
        txn.prnt_ean_num,
        g.ex_rt,
        g.to_crncy,
        txn.sls_amt,
        txn.sls_qty,
        txn.rtrn_qty,
        txn.rtrn_amt,
        txn.hq,
        txn.store_type,
        txn.sell_in_price_manual,
        txn.sell_out_unit_price,
        txn.dstr_nm
)
select * from final