with v_intrm_copa_sls_cust_hier as (
    select * from snapntaedw_integration.v_intrm_copa_sls_cust_hier
),
v_intrm_invc_sls_cust_hier as (
    select * from snapntaedw_integration.v_intrm_invc_sls_cust_hier
),
v_intrm_copa_sls as (
    SELECT v_intrm_copa_sls_cust_hier.ctry_nm,
        v_intrm_copa_sls_cust_hier.cust_num,
        v_intrm_copa_sls_cust_hier.cust_nm,
        v_intrm_copa_sls_cust_hier.matl_num,
        v_intrm_copa_sls_cust_hier.matl_desc,
        v_intrm_copa_sls_cust_hier.mega_brnd_desc,
        v_intrm_copa_sls_cust_hier.brnd_desc,
        v_intrm_copa_sls_cust_hier.ean_num,
        v_intrm_copa_sls_cust_hier.prod_hier_l1,
        v_intrm_copa_sls_cust_hier.prod_hier_l2,
        v_intrm_copa_sls_cust_hier.prod_hier_l3,
        v_intrm_copa_sls_cust_hier.prod_hier_l4,
        v_intrm_copa_sls_cust_hier.prod_hier_l5,
        v_intrm_copa_sls_cust_hier.prod_hier_l6,
        v_intrm_copa_sls_cust_hier.prod_hier_l7,
        v_intrm_copa_sls_cust_hier.prod_hier_l8,
        v_intrm_copa_sls_cust_hier.prod_hier_l9,
        v_intrm_copa_sls_cust_hier.sls_ofc_desc,
        v_intrm_copa_sls_cust_hier.channel,
        v_intrm_copa_sls_cust_hier.store_type,
        v_intrm_copa_sls_cust_hier.sls_grp,
        v_intrm_copa_sls_cust_hier.cust_hier_l1,
        v_intrm_copa_sls_cust_hier.cust_hier_l2,
        v_intrm_copa_sls_cust_hier.cust_hier_l3,
        v_intrm_copa_sls_cust_hier.cust_hier_l4,
        v_intrm_copa_sls_cust_hier.cust_hier_l5,
        v_intrm_copa_sls_cust_hier.fisc_yr_per,
        v_intrm_copa_sls_cust_hier.obj_crncy_co_obj,
        v_intrm_copa_sls_cust_hier.acct_hier_shrt_desc,
        v_intrm_copa_sls_cust_hier.copa_val,
        v_intrm_copa_sls_cust_hier.from_crncy,
        v_intrm_copa_sls_cust_hier.to_crncy,
        v_intrm_copa_sls_cust_hier.ex_rt_typ,
        v_intrm_copa_sls_cust_hier.ex_rt,
        v_intrm_copa_sls_cust_hier.no_of_days
    FROM v_intrm_copa_sls_cust_hier v_intrm_copa_sls_cust_hier
    WHERE (
            (v_intrm_copa_sls_cust_hier.acct_hier_shrt_desc)::text = ('TP'::character varying)::text
        )
),
a as (
    SELECT v_intrm_copa_sls.ctry_nm,
        COALESCE(
            v_intrm_copa_sls.cust_num,
            '#'::character varying
        ) AS cust_num,
        COALESCE(
            v_intrm_copa_sls.cust_nm,
            'Not Available'::character varying
        ) AS cust_nm,
        COALESCE(
            v_intrm_copa_sls.matl_num,
            '#'::character varying
        ) AS matl_num,
        COALESCE(
            v_intrm_copa_sls.matl_desc,
            'Not Available'::character varying
        ) AS matl_desc,
        COALESCE(
            v_intrm_copa_sls.mega_brnd_desc,
            'Not Available'::character varying
        ) AS mega_brnd_desc,
        COALESCE(
            v_intrm_copa_sls.brnd_desc,
            'Not Available'::character varying
        ) AS brnd_desc,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l1
        END AS prod_hier_l1,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l2
        END AS prod_hier_l2,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l3
        END AS prod_hier_l3,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l4
        END AS prod_hier_l4,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l5
        END AS prod_hier_l5,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l6 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l6)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l6
        END AS prod_hier_l6,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l7 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l7)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l7
        END AS prod_hier_l7,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l8 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l8)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l8
        END AS prod_hier_l8,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l9 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l9)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l9
        END AS prod_hier_l9,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l1
        END AS cust_hier_l1,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l2
        END AS cust_hier_l2,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l3
        END AS cust_hier_l3,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l4
        END AS cust_hier_l4,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l5
        END AS cust_hier_l5,
        COALESCE(v_intrm_copa_sls.sls_grp, '#'::character varying) AS sls_grp,
        COALESCE(
            v_intrm_copa_sls.sls_ofc_desc,
            '#'::character varying
        ) AS sls_ofc_desc,
        COALESCE(v_intrm_copa_sls.channel, '#'::character varying) AS channel,
        COALESCE(
            v_intrm_copa_sls.store_type,
            '#'::character varying
        ) AS store_type,
        v_intrm_copa_sls.fisc_yr_per,
        v_intrm_copa_sls.obj_crncy_co_obj,
        v_intrm_copa_sls.acct_hier_shrt_desc,
        sum(v_intrm_copa_sls.copa_val) AS copa_val,
        v_intrm_copa_sls.from_crncy,
        v_intrm_copa_sls.to_crncy,
        v_intrm_copa_sls.ex_rt_typ,
        v_intrm_copa_sls.ex_rt,
        v_intrm_copa_sls.no_of_days
    FROM v_intrm_copa_sls
    GROUP BY v_intrm_copa_sls.ctry_nm,
        COALESCE(
            v_intrm_copa_sls.cust_num,
            '#'::character varying
        ),
        COALESCE(
            v_intrm_copa_sls.cust_nm,
            'Not Available'::character varying
        ),
        COALESCE(
            v_intrm_copa_sls.matl_num,
            '#'::character varying
        ),
        COALESCE(
            v_intrm_copa_sls.matl_desc,
            'Not Available'::character varying
        ),
        COALESCE(
            v_intrm_copa_sls.mega_brnd_desc,
            'Not Available'::character varying
        ),
        COALESCE(
            v_intrm_copa_sls.brnd_desc,
            'Not Available'::character varying
        ),
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l1
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l2
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l3
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l4
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l5
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l6 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l6)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l6
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l7 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l7)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l7
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l8 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l8)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l8
        END,
        CASE
            WHEN (v_intrm_copa_sls.prod_hier_l9 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.prod_hier_l9)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.prod_hier_l9
        END,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l1
        END,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l2
        END,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l3
        END,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l4
        END,
        CASE
            WHEN (v_intrm_copa_sls.cust_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_copa_sls.cust_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_copa_sls.cust_hier_l5
        END,
        COALESCE(v_intrm_copa_sls.sls_grp, '#'::character varying),
        COALESCE(
            v_intrm_copa_sls.sls_ofc_desc,
            '#'::character varying
        ),
        COALESCE(v_intrm_copa_sls.channel, '#'::character varying),
        COALESCE(
            v_intrm_copa_sls.store_type,
            '#'::character varying
        ),
        v_intrm_copa_sls.fisc_yr_per,
        v_intrm_copa_sls.obj_crncy_co_obj,
        v_intrm_copa_sls.acct_hier_shrt_desc,
        v_intrm_copa_sls.from_crncy,
        v_intrm_copa_sls.to_crncy,
        v_intrm_copa_sls.ex_rt_typ,
        v_intrm_copa_sls.ex_rt,
        v_intrm_copa_sls.no_of_days
),
b as (
    SELECT v_intrm_invc_sls.ctry_nm,
        COALESCE(
            v_intrm_invc_sls.cust_num,
            '#'::character varying
        ) AS cust_num,
        COALESCE(
            v_intrm_invc_sls.cust_nm,
            'Not Available'::character varying
        ) AS cust_nm,
        COALESCE(
            v_intrm_invc_sls.matl_num,
            '#'::character varying
        ) AS matl_num,
        COALESCE(
            v_intrm_invc_sls.matl_desc,
            'Not Available'::character varying
        ) AS matl_desc,
        COALESCE(
            v_intrm_invc_sls.mega_brnd_desc,
            'Not Available'::character varying
        ) AS mega_brnd_desc,
        COALESCE(
            v_intrm_invc_sls.brnd_desc,
            'Not Available'::character varying
        ) AS brnd_desc,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l1
        END AS prod_hier_l1,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l2
        END AS prod_hier_l2,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l3
        END AS prod_hier_l3,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l4
        END AS prod_hier_l4,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l5
        END AS prod_hier_l5,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l6 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l6)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l6
        END AS prod_hier_l6,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l7 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l7)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l7
        END AS prod_hier_l7,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l8 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l8)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l8
        END AS prod_hier_l8,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l9 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l9)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l9
        END AS prod_hier_l9,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l1
        END AS cust_hier_l1,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l2
        END AS cust_hier_l2,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l3
        END AS cust_hier_l3,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l4
        END AS cust_hier_l4,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l5
        END AS cust_hier_l5,
        COALESCE(v_intrm_invc_sls.sls_grp, '#'::character varying) AS sls_grp,
        COALESCE(
            v_intrm_invc_sls.sls_ofc_desc,
            '#'::character varying
        ) AS sls_ofc_desc,
        COALESCE(v_intrm_invc_sls.channel, '#'::character varying) AS channel,
        v_intrm_invc_sls.fisc_yr,
        sum(v_intrm_invc_sls.net_bill_val) AS net_bill_val,
        sum(v_intrm_invc_sls.ord_qty_pc) AS ord_qty_pc,
        v_intrm_invc_sls.from_crncy,
        v_intrm_invc_sls.to_crncy,
        v_intrm_invc_sls.ex_rt_typ,
        v_intrm_invc_sls.ex_rt
    FROM v_intrm_invc_sls_cust_hier v_intrm_invc_sls
    GROUP BY v_intrm_invc_sls.ctry_nm,
        COALESCE(
            v_intrm_invc_sls.cust_num,
            '#'::character varying
        ),
        COALESCE(
            v_intrm_invc_sls.cust_nm,
            'Not Available'::character varying
        ),
        COALESCE(
            v_intrm_invc_sls.matl_num,
            '#'::character varying
        ),
        COALESCE(
            v_intrm_invc_sls.matl_desc,
            'Not Available'::character varying
        ),
        COALESCE(
            v_intrm_invc_sls.mega_brnd_desc,
            'Not Available'::character varying
        ),
        COALESCE(
            v_intrm_invc_sls.brnd_desc,
            'Not Available'::character varying
        ),
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l1
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l2
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l3
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l4
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l5
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l6 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l6)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l6
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l7 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l7)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l7
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l8 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l8)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l8
        END,
        CASE
            WHEN (v_intrm_invc_sls.prod_hier_l9 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.prod_hier_l9)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.prod_hier_l9
        END,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l1 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l1)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l1
        END,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l2 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l2)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l2
        END,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l3 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l3)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l3
        END,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l4 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l4)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l4
        END,
        CASE
            WHEN (v_intrm_invc_sls.cust_hier_l5 IS NULL) THEN '#'::character varying
            WHEN (
                (v_intrm_invc_sls.cust_hier_l5)::text = (''::character varying)::text
            ) THEN '#'::character varying
            ELSE v_intrm_invc_sls.cust_hier_l5
        END,
        COALESCE(v_intrm_invc_sls.sls_grp, '#'::character varying),
        COALESCE(
            v_intrm_invc_sls.sls_ofc_desc,
            '#'::character varying
        ),
        COALESCE(v_intrm_invc_sls.channel, '#'::character varying),
        v_intrm_invc_sls.fisc_yr,
        v_intrm_invc_sls.from_crncy,
        v_intrm_invc_sls.to_crncy,
        v_intrm_invc_sls.ex_rt_typ,
        v_intrm_invc_sls.ex_rt
),
derived_table1 as (
    SELECT COALESCE(a.ctry_nm, b.ctry_nm) AS ctry_nm,
        COALESCE(a.cust_num, b.cust_num) AS cust_num,
        COALESCE(a.cust_nm, b.cust_nm) AS cust_nm,
        COALESCE(a.matl_num, b.matl_num) AS matl_num,
        COALESCE(a.matl_desc, b.matl_desc) AS matl_desc,
        COALESCE(a.mega_brnd_desc, b.mega_brnd_desc) AS mega_brnd_desc,
        COALESCE(a.brnd_desc, b.brnd_desc) AS brnd_desc,
        COALESCE(
            (a.ctry_nm)::character varying(500),
            b.prod_hier_l1
        ) AS prod_hier_l1,
        COALESCE(a.prod_hier_l2, b.prod_hier_l2) AS prod_hier_l2,
        COALESCE(a.prod_hier_l3, b.prod_hier_l3) AS prod_hier_l3,
        COALESCE(a.prod_hier_l4, b.prod_hier_l4) AS prod_hier_l4,
        COALESCE(a.prod_hier_l5, b.prod_hier_l5) AS prod_hier_l5,
        COALESCE(a.prod_hier_l6, b.prod_hier_l6) AS prod_hier_l6,
        COALESCE(a.prod_hier_l7, b.prod_hier_l7) AS prod_hier_l7,
        COALESCE(a.prod_hier_l8, b.prod_hier_l8) AS prod_hier_l8,
        COALESCE(a.prod_hier_l9, b.prod_hier_l9) AS prod_hier_l9,
        COALESCE(a.cust_hier_l1, b.cust_hier_l1) AS cust_hier_l1,
        COALESCE(a.cust_hier_l2, b.cust_hier_l2) AS cust_hier_l2,
        COALESCE(a.cust_hier_l3, b.cust_hier_l3) AS cust_hier_l3,
        COALESCE(a.cust_hier_l4, b.cust_hier_l4) AS cust_hier_l4,
        COALESCE(a.cust_hier_l5, b.cust_hier_l5) AS cust_hier_l5,
        COALESCE(a.sls_grp, b.sls_grp) AS sls_grp,
        COALESCE(a.sls_ofc_desc, b.sls_ofc_desc) AS sls_ofc_desc,
        COALESCE(a.channel, b.channel) AS channel,
        COALESCE(a.store_type, '#'::character varying) AS store_type,
        COALESCE(
            (a.fisc_yr_per)::character varying(34),
            b.fisc_yr
        ) AS fisc_yr_per,
        COALESCE(a.acct_hier_shrt_desc, 'TP'::character varying) AS acct_hier_shrt_desc,
        COALESCE(a.from_crncy, b.from_crncy) AS from_crncy,
        COALESCE(a.to_crncy, b.to_crncy) AS to_crncy,
        COALESCE(a.ex_rt_typ, b.ex_rt_typ) AS ex_rt_typ,
        COALESCE(a.ex_rt, b.ex_rt) AS ex_rt,
        COALESCE(a.copa_val, 0.0) AS copa_val,
        COALESCE(b.net_bill_val, 0.0) AS net_bill_val,
        COALESCE(b.ord_qty_pc, 0.0) AS ord_qty_pc
    FROM a
        FULL JOIN b ON ((a.ctry_nm)::text = (b.ctry_nm)::text)
        AND trim(ltrim((a.cust_num)::text, ((0)::character varying)::text)) = trim(ltrim((b.cust_num)::text, ((0)::character varying)::text))
        AND trim((a.cust_nm)::text) = trim((b.cust_nm)::text)
        AND trim(ltrim((a.matl_num)::text, ((0)::character varying)::text)) = trim(ltrim((b.matl_num)::text, ((0)::character varying)::text))
        AND trim((a.matl_desc)::text) = trim((b.matl_desc)::text)
        AND (a.mega_brnd_desc)::text = (b.mega_brnd_desc)::text
        AND ((a.brnd_desc)::text = (b.brnd_desc)::text)
        AND ((a.prod_hier_l1)::text = (b.prod_hier_l1)::text)
        AND ((a.prod_hier_l2)::text = (b.prod_hier_l2)::text)
        AND ((a.prod_hier_l3)::text = (b.prod_hier_l3)::text)
        AND ((a.prod_hier_l4)::text = (b.prod_hier_l4)::text)
        AND ((a.prod_hier_l5)::text = (b.prod_hier_l5)::text)
        AND ((a.prod_hier_l6)::text = (b.prod_hier_l6)::text)
        AND ((a.prod_hier_l7)::text = (b.prod_hier_l7)::text)
        AND ((a.prod_hier_l8)::text = (b.prod_hier_l8)::text)
        AND ((a.prod_hier_l9)::text = (b.prod_hier_l9)::text)
        AND ((a.cust_hier_l1)::text = (b.cust_hier_l1)::text)
        AND ((a.cust_hier_l2)::text = (b.cust_hier_l2)::text)
        AND ((a.cust_hier_l3)::text = (b.cust_hier_l3)::text)
        AND ((a.cust_hier_l4)::text = (b.cust_hier_l4)::text)
        AND ((a.cust_hier_l5)::text = (b.cust_hier_l5)::text)
        AND ((a.sls_grp)::text = (b.sls_grp)::text)
        AND ((a.sls_ofc_desc)::text = (b.sls_ofc_desc)::text)
        AND ((a.channel)::text = (b.channel)::text)
        AND ((a.fisc_yr_per)::character varying)::text = (b.fisc_yr)::text
        AND (a.obj_crncy_co_obj)::text = (b.from_crncy)::text
        AND ((a.from_crncy)::text = (b.from_crncy)::text)
        AND ((a.to_crncy)::text = (b.to_crncy)::text)
        AND ((a.ex_rt_typ)::text = (b.ex_rt_typ)::text)
        AND (a.ex_rt = b.ex_rt)
),
final as (
    SELECT derived_table1.ctry_nm,
        derived_table1.cust_num,
        derived_table1.cust_nm,
        derived_table1.matl_num,
        derived_table1.matl_desc,
        derived_table1.mega_brnd_desc,
        derived_table1.brnd_desc,
        derived_table1.prod_hier_l1,
        derived_table1.prod_hier_l2,
        derived_table1.prod_hier_l3,
        derived_table1.prod_hier_l4,
        derived_table1.prod_hier_l5,
        derived_table1.prod_hier_l6,
        derived_table1.prod_hier_l7,
        derived_table1.prod_hier_l8,
        derived_table1.prod_hier_l9,
        derived_table1.cust_hier_l1,
        derived_table1.cust_hier_l2,
        derived_table1.cust_hier_l3,
        derived_table1.cust_hier_l4,
        derived_table1.cust_hier_l5,
        derived_table1.sls_grp,
        derived_table1.sls_ofc_desc,
        derived_table1.channel,
        derived_table1.store_type,
        derived_table1.fisc_yr_per,
        derived_table1.acct_hier_shrt_desc,
        derived_table1.from_crncy,
        derived_table1.to_crncy,
        derived_table1.ex_rt_typ,
        derived_table1.ex_rt,
        derived_table1.copa_val,
        derived_table1.net_bill_val,
        derived_table1.ord_qty_pc,
        CASE
            WHEN (
                (
                    (
                        "substring"((derived_table1.fisc_yr_per)::text, 1, 4)
                    )::integer = "date_part"(
                        year,
                        current_timestamp()
                    )
                )
                AND (
                    (
                        "substring"((derived_table1.fisc_yr_per)::text, 6, 2)
                    )::integer = "date_part"(
                        month,
                        current_timestamp()
                    )
                )
            ) THEN (
                (
                    (
                        (
                            (
                                "date_part"(
                                    day,
                                    current_timestamp()
                                )
                            )::numeric
                        )::numeric(18, 0)
                    )::numeric(15, 0) / (
                        (
                            "date_part"(
                                day,
                                last_day(
                                    (
                                        (
                                            (
                                                (
                                                    "substring"((derived_table1.fisc_yr_per)::text, 1, 4) || "substring"((derived_table1.fisc_yr_per)::text, 6, 7)
                                                ) || ('01'::character varying)::text
                                            )
                                        )::date
                                    )::timestamp without time zone
                                )
                            )
                        )::numeric
                    )::numeric(18, 0)
                ) * ((100)::numeric)::numeric(18, 0)
            )
            WHEN (
                (
                    (
                        "substring"((derived_table1.fisc_yr_per)::text, 1, 4)
                    )::integer = "date_part"(
                        year,
                        current_timestamp()
                    )
                )
                AND (
                    (
                        "substring"((derived_table1.fisc_yr_per)::text, 6, 2)
                    )::integer > "date_part"(
                        month,
                        current_timestamp()
                    )
                )
            ) THEN ((999)::numeric)::numeric(18, 0)
            ELSE ((100)::numeric)::numeric(18, 0)
        END AS timegone
    FROM derived_table1
)
select * from final