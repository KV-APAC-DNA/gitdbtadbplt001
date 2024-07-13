with v_intrm_curr_copa_plan_denorm_le as(
    select * from {{ ref('ntaedw_integration__v_intrm_curr_copa_plan_denorm_le') }}
),
v_intrm_curr_copa_plan_denorm_rf as(
    select * from {{ ref('ntaedw_integration__v_intrm_curr_copa_plan_denorm_rf') }}
),
v_intrm_curr_copa_plan_denorm_bp as(
    select * from {{ ref('ntaedw_integration__v_intrm_curr_copa_plan_denorm_bp') }}
),
final as
(   
    SELECT 
        a.ctry_nm,
        a.cust_num,
        a.cust_nm,
        a.matl_num,
        a.matl_desc,
        a.ean_num,
        a.mega_brnd_desc,
        a.brnd_desc,
        a.prod_hier_l1,
        a.prod_hier_l2,
        a.prod_hier_l3,
        a.prod_hier_l4,
        a.prod_hier_l5,
        a.prod_hier_l6,
        a.prod_hier_l7,
        a.prod_hier_l8,
        a.prod_hier_l9,
        a.fisc_yr_per,
        a.acct_hier_shrt_desc,
        a.from_crncy,
        a.to_crncy,
        a.ex_rt,
        a.rf_total,
        a.bp_total,
        a.le_total,
        null AS sls_ofc_desc,
        null AS sls_grp,
        null AS channel,
        null AS store_type,
        null AS cust_hier_l1,
        null AS cust_hier_l2,
        null AS cust_hier_l3,
        null AS cust_hier_l4,
        null AS cust_hier_l5
    FROM 
        (
            SELECT 
                DISTINCT 
                CASE
                    WHEN (a.ctry_nm IS NOT NULL) THEN a.ctry_nm
                    WHEN ((a.ctry_nm IS NULL)AND (b.ctry_nm IS NOT NULL)) THEN b.ctry_nm
                    WHEN ((a.ctry_nm IS NULL)AND (b.ctry_nm IS NULL)) THEN c.ctry_nm
                    ELSE NULL::character varying
                END AS ctry_nm,
                CASE
                    WHEN (a.cust_num IS NOT NULL) THEN a.cust_num
                    WHEN ((a.cust_num IS NULL)AND (b.cust_num IS NOT NULL)) THEN b.cust_num
                    WHEN ((a.cust_num IS NULL)AND (b.cust_num IS NULL)) THEN c.cust_num
                    ELSE NULL::character varying
                END AS cust_num,
                CASE
                    WHEN (a.cust_nm IS NOT NULL) THEN a.cust_nm
                    WHEN ((a.cust_nm IS NULL)AND (b.cust_nm IS NOT NULL)) THEN b.cust_nm
                    WHEN ((a.cust_nm IS NULL)AND (b.cust_nm IS NULL)) THEN c.cust_nm
                    ELSE NULL::character varying
                END AS cust_nm,
                CASE
                    WHEN (a.matl_num IS NOT NULL) THEN a.matl_num
                    WHEN ((a.matl_num IS NULL)AND (b.matl_num IS NOT NULL)) THEN b.matl_num
                    WHEN ((a.matl_num IS NULL)AND (b.matl_num IS NULL)) THEN c.matl_num
                    ELSE NULL::character varying
                END AS matl_num,
                CASE
                    WHEN (a.matl_desc IS NOT NULL) THEN a.matl_desc
                    WHEN ((a.matl_desc IS NULL)AND (b.matl_desc IS NOT NULL)) THEN b.matl_desc
                    WHEN ((a.matl_desc IS NULL)AND (b.matl_desc IS NULL)) THEN c.matl_desc
                    ELSE NULL::character varying
                END AS matl_desc,
                CASE
                    WHEN (a.ean_num IS NOT NULL) THEN a.ean_num
                    WHEN ((a.ean_num IS NULL)AND (b.ean_num IS NOT NULL)) THEN b.ean_num
                    WHEN ((a.ean_num IS NULL)AND (b.ean_num IS NULL)) THEN c.ean_num
                    ELSE NULL::character varying
                END AS ean_num,
                CASE
                    WHEN (a.mega_brnd_desc IS NOT NULL) THEN a.mega_brnd_desc
                    WHEN ((a.mega_brnd_desc IS NULL)AND (b.mega_brnd_desc IS NOT NULL)) THEN b.mega_brnd_desc
                    WHEN ((a.mega_brnd_desc IS NULL)AND (b.mega_brnd_desc IS NULL)) THEN c.mega_brnd_desc
                    ELSE NULL::character varying
                END AS mega_brnd_desc,
                CASE
                    WHEN (a.brnd_desc IS NOT NULL) THEN a.brnd_desc
                    WHEN ((a.brnd_desc IS NULL)AND (b.brnd_desc IS NOT NULL)) THEN b.brnd_desc
                    WHEN ((a.brnd_desc IS NULL)AND (b.brnd_desc IS NULL)) THEN c.brnd_desc
                    ELSE NULL::character varying
                END AS brnd_desc,
                CASE
                    WHEN (a.prod_hier_l1 IS NOT NULL) THEN a.prod_hier_l1
                    WHEN ((a.prod_hier_l1 IS NULL)AND (b.prod_hier_l1 IS NOT NULL)) THEN b.prod_hier_l1
                    WHEN ((a.prod_hier_l1 IS NULL)AND (b.prod_hier_l1 IS NULL)) THEN c.prod_hier_l1
                    ELSE NULL::character varying
                END AS prod_hier_l1,
                CASE
                    WHEN (a.prod_hier_l2 IS NOT NULL) THEN a.prod_hier_l2
                    WHEN ((a.prod_hier_l2 IS NULL) AND (b.prod_hier_l2 IS NOT NULL)) THEN b.prod_hier_l2
                    WHEN ((a.prod_hier_l2 IS NULL) AND (b.prod_hier_l2 IS NULL)) THEN c.prod_hier_l2
                    ELSE NULL::character varying
                END AS prod_hier_l2,
                CASE
                    WHEN (a.prod_hier_l3 IS NOT NULL) THEN a.prod_hier_l3
                    WHEN ((a.prod_hier_l3 IS NULL) AND (b.prod_hier_l3 IS NOT NULL)) THEN b.prod_hier_l3
                    WHEN ((a.prod_hier_l3 IS NULL) AND (b.prod_hier_l3 IS NULL)) THEN c.prod_hier_l3
                    ELSE NULL::character varying
                END AS prod_hier_l3,
                CASE
                    WHEN (a.prod_hier_l4 IS NOT NULL) THEN a.prod_hier_l4
                    WHEN ((a.prod_hier_l4 IS NULL) AND (b.prod_hier_l4 IS NOT NULL)) THEN b.prod_hier_l4
                    WHEN ((a.prod_hier_l4 IS NULL) AND (b.prod_hier_l4 IS NULL)) THEN c.prod_hier_l4
                    ELSE NULL::character varying
                END AS prod_hier_l4,
                CASE
                    WHEN (a.prod_hier_l5 IS NOT NULL) THEN a.prod_hier_l5
                    WHEN ((a.prod_hier_l5 IS NULL) AND (b.prod_hier_l5 IS NOT NULL)) THEN b.prod_hier_l5
                    WHEN ((a.prod_hier_l5 IS NULL) AND (b.prod_hier_l5 IS NULL)) THEN c.prod_hier_l5
                    ELSE NULL::character varying
                END AS prod_hier_l5,
                CASE
                    WHEN (a.prod_hier_l6 IS NOT NULL) THEN a.prod_hier_l6
                    WHEN ((a.prod_hier_l6 IS NULL) AND (b.prod_hier_l6 IS NOT NULL)) THEN b.prod_hier_l6
                    WHEN ((a.prod_hier_l6 IS NULL) AND (b.prod_hier_l6 IS NULL)) THEN c.prod_hier_l6
                    ELSE NULL::character varying
                END AS prod_hier_l6,
                CASE
                    WHEN (a.prod_hier_l7 IS NOT NULL) THEN a.prod_hier_l7
                    WHEN ((a.prod_hier_l7 IS NULL) AND (b.prod_hier_l7 IS NOT NULL)) THEN b.prod_hier_l7
                    WHEN ((a.prod_hier_l7 IS NULL) AND (b.prod_hier_l7 IS NULL)) THEN c.prod_hier_l7
                    ELSE NULL::character varying
                END AS prod_hier_l7,
                CASE
                    WHEN (a.prod_hier_l8 IS NOT NULL) THEN a.prod_hier_l8
                    WHEN ((a.prod_hier_l8 IS NULL) AND (b.prod_hier_l8 IS NOT NULL)) THEN b.prod_hier_l8
                    WHEN ((a.prod_hier_l8 IS NULL) AND (b.prod_hier_l8 IS NULL)) THEN c.prod_hier_l8
                    ELSE NULL::character varying
                END AS prod_hier_l8,
                CASE
                    WHEN (a.prod_hier_l9 IS NOT NULL) THEN a.prod_hier_l9
                    WHEN ((a.prod_hier_l9 IS NULL) AND (b.prod_hier_l9 IS NOT NULL)) THEN b.prod_hier_l9
                    WHEN ((a.prod_hier_l9 IS NULL) AND (b.prod_hier_l9 IS NULL)) THEN c.prod_hier_l9
                    ELSE NULL::character varying
                END AS prod_hier_l9,
                CASE
                    WHEN (a.fisc_yr_per IS NOT NULL) THEN a.fisc_yr_per
                    WHEN ((a.fisc_yr_per IS NULL) AND (b.fisc_yr_per IS NOT NULL)) THEN b.fisc_yr_per
                    WHEN ((a.fisc_yr_per IS NULL) AND (b.fisc_yr_per IS NULL)) THEN c.fisc_yr_per
                    ELSE (NULL::numeric)::numeric(18, 0)
                END AS fisc_yr_per,
                CASE
                    WHEN (a.acct_hier_shrt_desc IS NOT NULL) THEN a.acct_hier_shrt_desc
                    WHEN ((a.acct_hier_shrt_desc IS NULL) AND (b.acct_hier_shrt_desc IS NOT NULL)) THEN b.acct_hier_shrt_desc
                    WHEN ((a.acct_hier_shrt_desc IS NULL) AND (b.acct_hier_shrt_desc IS NULL)) THEN c.acct_hier_shrt_desc
                    ELSE NULL::character varying
                END AS acct_hier_shrt_desc,
                CASE
                    WHEN (a.from_crncy IS NOT NULL) THEN a.from_crncy
                    WHEN ((a.from_crncy IS NULL) AND (b.from_crncy IS NOT NULL)) THEN b.from_crncy
                    WHEN ((a.from_crncy IS NULL) AND (b.from_crncy IS NULL)) THEN c.from_crncy
                    ELSE NULL::character varying
                END AS from_crncy,
                CASE
                    WHEN (a.to_crncy IS NOT NULL) THEN a.to_crncy
                    WHEN ((a.to_crncy IS NULL) AND (b.to_crncy IS NOT NULL)) THEN b.to_crncy
                    WHEN ((a.to_crncy IS NULL) AND (b.to_crncy IS NULL)) THEN c.to_crncy
                    ELSE NULL::character varying
                END AS to_crncy,
                CASE
                    WHEN (a.ex_rt IS NOT NULL) THEN a.ex_rt
                    WHEN ((a.ex_rt IS NULL) AND (b.ex_rt IS NOT NULL)) THEN b.ex_rt
                    WHEN ((a.ex_rt IS NULL) AND (b.ex_rt IS NULL)) THEN c.ex_rt
                    ELSE (NULL::numeric)::numeric(18, 0)
                END AS ex_rt,
                COALESCE(b.rf_total, 0.0) AS rf_total,
                COALESCE(c.bp_total, 0.0) AS bp_total,
                COALESCE(a.le_total, 0.0) AS le_total
            FROM 
                (
                    (
                        SELECT DISTINCT z.ctry_nm,
                            z.cust_num,
                            z.cust_nm,
                            z.matl_num,
                            z.matl_desc,
                            z.ean_num,
                            COALESCE(
                                z.mega_brnd_desc,
                                'Not Available'::character varying
                            ) AS mega_brnd_desc,
                            COALESCE(z.brnd_desc, 'Not Available'::character varying) AS brnd_desc,
                            COALESCE(z.prod_hier_l1, '#'::character varying) AS prod_hier_l1,
                            COALESCE(z.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
                            COALESCE(z.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
                            COALESCE(z.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
                            COALESCE(z.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
                            COALESCE(z.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
                            COALESCE(z.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
                            COALESCE(z.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
                            COALESCE(z.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
                            z.acct_hier_shrt_desc,
                            z.fisc_yr_per,
                            z.le_total,
                            z.from_crncy,
                            z.to_crncy,
                            z.ex_rt
                        FROM v_intrm_curr_copa_plan_denorm_le z
                    ) a
                    FULL JOIN 
                    (
                        SELECT DISTINCT x.ctry_nm,
                            x.cust_num,
                            x.cust_nm,
                            x.matl_num,
                            x.matl_desc,
                            x.ean_num,
                            COALESCE(
                                x.mega_brnd_desc,
                                'Not Available'::character varying
                            ) AS mega_brnd_desc,
                            COALESCE(x.brnd_desc, 'Not Available'::character varying) AS brnd_desc,
                            COALESCE(x.prod_hier_l1, '#'::character varying) AS prod_hier_l1,
                            COALESCE(x.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
                            COALESCE(x.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
                            COALESCE(x.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
                            COALESCE(x.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
                            COALESCE(x.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
                            COALESCE(x.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
                            COALESCE(x.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
                            COALESCE(x.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
                            x.fisc_yr_per,
                            x.acct_hier_shrt_desc,
                            x.rf_total,
                            x.from_crncy,
                            x.to_crncy,
                            x.ex_rt
                        FROM v_intrm_curr_copa_plan_denorm_rf x
                    ) b ON 
                    (
                        (a.ctry_nm)::text = (b.ctry_nm)::text
                        AND trim(ltrim((a.cust_num)::text, '0')) = trim(ltrim((b.cust_num)::text, '0'))
                        AND trim((a.cust_nm)::text) = trim((b.cust_nm)::text)
                        AND trim(ltrim((a.matl_num)::text, '0')) = trim(ltrim((b.matl_num)::text, '0'))
                        AND trim((a.matl_desc)::text) = trim((b.matl_desc)::text)
                        AND (a.mega_brnd_desc)::text = (b.mega_brnd_desc)::text
                        AND (a.brnd_desc)::text = (b.brnd_desc)::text
                        AND (COALESCE(a.ean_num, '#'))::text = (COALESCE(b.ean_num, '#'))::text
                        AND ((a.fisc_yr_per)::character varying)::numeric = b.fisc_yr_per
                        AND (a.from_crncy)::text = (b.from_crncy)::text
                        AND (a.acct_hier_shrt_desc)::text = (b.acct_hier_shrt_desc)::text
                        AND (a.to_crncy)::text = (b.to_crncy)::text
                    )
                    FULL JOIN 
                    (
                        SELECT DISTINCT y.ctry_nm,
                            y.cust_num,
                            y.cust_nm,
                            y.matl_num,
                            y.matl_desc,
                            y.ean_num,
                            COALESCE(
                                y.mega_brnd_desc,
                                'Not Available'::character varying
                            ) AS mega_brnd_desc,
                            COALESCE(y.brnd_desc, 'Not Available'::character varying) AS brnd_desc,
                            COALESCE(y.prod_hier_l1, '#'::character varying) AS prod_hier_l1,
                            COALESCE(y.prod_hier_l2, '#'::character varying) AS prod_hier_l2,
                            COALESCE(y.prod_hier_l3, '#'::character varying) AS prod_hier_l3,
                            COALESCE(y.prod_hier_l4, '#'::character varying) AS prod_hier_l4,
                            COALESCE(y.prod_hier_l5, '#'::character varying) AS prod_hier_l5,
                            COALESCE(y.prod_hier_l6, '#'::character varying) AS prod_hier_l6,
                            COALESCE(y.prod_hier_l7, '#'::character varying) AS prod_hier_l7,
                            COALESCE(y.prod_hier_l8, '#'::character varying) AS prod_hier_l8,
                            COALESCE(y.prod_hier_l9, '#'::character varying) AS prod_hier_l9,
                            y.acct_hier_shrt_desc,
                            y.fisc_yr_per,
                            y.bp_total,
                            y.from_crncy,
                            y.to_crncy,
                            y.ex_rt
                        FROM v_intrm_curr_copa_plan_denorm_bp y
                    ) c ON 
                    (
                        (a.ctry_nm)::text = (c.ctry_nm)::text
                        AND trim(ltrim((a.cust_num)::text, '0')) = trim(ltrim((c.cust_num)::text, '0'))
                        AND trim((a.cust_nm)::text) = trim((c.cust_nm)::text)
                        AND trim(ltrim((a.matl_num)::text, '0')) = trim(ltrim((c.matl_num)::text, '0'))
                        AND trim((a.matl_desc)::text) = trim((c.matl_desc)::text)
                        AND (a.mega_brnd_desc)::text = (c.mega_brnd_desc)::text
                        AND (a.brnd_desc)::text = (c.brnd_desc)::text
                        AND (COALESCE(a.ean_num, '#'))::text = (COALESCE(c.ean_num, '#'))::text
                        AND b.fisc_yr_per = c.fisc_yr_per
                        AND (a.from_crncy)::text = (c.from_crncy)::text
                        AND (a.acct_hier_shrt_desc)::text = (c.acct_hier_shrt_desc)::text
                        AND (a.to_crncy)::text = (c.to_crncy)::text
                    )
                )
        ) a
)
select * from final