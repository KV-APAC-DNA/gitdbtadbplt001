with edw_copa_plan_fact as (
    select * from snapaspedw_integration.edw_copa_plan_fact
),
v_intrm_crncy_exch as (
    select * from snapntaedw_integration.v_intrm_crncy_exch
),
edw_material_sales_dim as (
    select * from snapaspedw_integration.edw_material_sales_dim
),
edw_product_attr_dim as (
    select * from snapaspedw_integration.edw_product_attr_dim
),
edw_company_dim as (
    select * from snapaspedw_integration.edw_company_dim
),
edw_customer_base_dim as (
    select * from snapaspedw_integration.edw_customer_base_dim
),
edw_material_dim as (
    select * from snapaspedw_integration.edw_material_dim
),
cte1 as (
    SELECT p.obj_crncy,
        p.vers,
        p.fisc_yr,
        p.fisc_yr_per,
        p.category,
        p.freq,
        p.matl_num,
        p.sls_org,
        p.dstn_chnl,
        p.ctry_key,
        p.cust_num,
        p.co_cd,
        p.acct_hier_shrt_desc,
        p.acct_num,
        p.plan_amt
    FROM (
            SELECT x.obj_crncy,
                x.vers,
                x.fisc_yr,
                x.fisc_yr_per,
                x.category,
                'MTD'::character varying AS freq,
                x.matl_num,
                x.sls_org,
                x.dstn_chnl,
                x.ctry_key,
                x.cust_num,
                x.co_cd,
                'TP'::character varying AS acct_hier_shrt_desc,
                x.acct_num,
                CASE
                    WHEN (
                        (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                    ) THEN (
                        (- ((1)::numeric)::numeric(18, 0)) * x.amt_obj_crcy
                    )
                    ELSE x.amt_obj_crcy
                END AS plan_amt
            FROM edw_copa_plan_fact x
            WHERE (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                (
                                                                                    (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                                                                )
                                                                                OR (
                                                                                    (x.acct_hier_shrt_desc)::text = ('CD'::character varying)::text
                                                                                )
                                                                            )
                                                                            OR (
                                                                                (x.acct_hier_shrt_desc)::text = ('LF'::character varying)::text
                                                                            )
                                                                        )
                                                                        OR (
                                                                            (x.acct_hier_shrt_desc)::text = ('PSO'::character varying)::text
                                                                        )
                                                                    )
                                                                    OR (
                                                                        (x.acct_hier_shrt_desc)::text = ('PMA'::character varying)::text
                                                                    )
                                                                )
                                                                OR (
                                                                    (x.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                                                )
                                                            )
                                                            OR (
                                                                (x.acct_hier_shrt_desc)::text = ('SA'::character varying)::text
                                                            )
                                                        )
                                                        OR (
                                                            (x.acct_hier_shrt_desc)::text = ('TLO'::character varying)::text
                                                        )
                                                    )
                                                    OR (
                                                        (x.acct_hier_shrt_desc)::text = ('VGF'::character varying)::text
                                                    )
                                                )
                                                OR (
                                                    (x.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                                )
                                            )
                                            OR (
                                                (x.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                            )
                                        )
                                        OR (
                                            (x.acct_hier_shrt_desc)::text = ('GC'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (x.acct_hier_shrt_desc)::text = ('NGC'::character varying)::text
                                    )
                                )
                                OR (
                                    (x.acct_hier_shrt_desc)::text = ('CTD'::character varying)::text
                                )
                            )
                            OR (
                                (x.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                            )
                        )
                        AND (
                            (x.category)::text = ('RF'::character varying)::text
                        )
                    )
                    AND (
                        (
                            (
                                (x.ctry_key)::text = ('HK'::character varying)::text
                            )
                            OR (
                                (x.ctry_key)::text = ('KR'::character varying)::text
                            )
                        )
                        OR (
                            (x.ctry_key)::text = ('TW'::character varying)::text
                        )
                    )
                )
            UNION ALL
            SELECT y.obj_crncy,
                y.vers,
                y.fisc_yr,
                y.fisc_yr_per,
                y.category,
                'MTD'::character varying AS freq,
                y.matl_num,
                y.sls_org,
                y.dstn_chnl,
                y.ctry_key,
                y.cust_num,
                y.co_cd,
                y.acct_hier_shrt_desc,
                y.acct_num,
                y.amt_obj_crcy AS plan_amt
            FROM edw_copa_plan_fact y
            WHERE (
                    (
                        (
                            (
                                (y.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                            )
                            OR (
                                (y.acct_hier_shrt_desc)::text = ('NTS'::character varying)::text
                            )
                        )
                        AND (
                            (y.category)::text = ('RF'::character varying)::text
                        )
                    )
                    AND (
                        (
                            (
                                (y.ctry_key)::text = ('HK'::character varying)::text
                            )
                            OR (
                                (y.ctry_key)::text = ('KR'::character varying)::text
                            )
                        )
                        OR (
                            (y.ctry_key)::text = ('TW'::character varying)::text
                        )
                    )
                )
        ) p
),
cte2 as (
    SELECT x.obj_crncy,
        x.vers,
        x.fisc_yr,
        x.fisc_yr_per,
        x.category,
        x.freq,
        x.matl_num,
        x.sls_org,
        x.dstn_chnl,
        x.ctry_key,
        x.cust_num,
        x.co_cd,
        'TP'::character varying AS acct_hier_shrt_desc,
        x.acct_num,
        CASE
            WHEN (
                (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
            ) THEN (
                (- ((1)::numeric)::numeric(18, 0)) * x.amt_obj_crcy
            )
            ELSE x.amt_obj_crcy
        END AS plan_amt
    FROM edw_copa_plan_fact x
    WHERE (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                                                        )
                                                                        OR (
                                                                            (x.acct_hier_shrt_desc)::text = ('CD'::character varying)::text
                                                                        )
                                                                    )
                                                                    OR (
                                                                        (x.acct_hier_shrt_desc)::text = ('LF'::character varying)::text
                                                                    )
                                                                )
                                                                OR (
                                                                    (x.acct_hier_shrt_desc)::text = ('PSO'::character varying)::text
                                                                )
                                                            )
                                                            OR (
                                                                (x.acct_hier_shrt_desc)::text = ('PMA'::character varying)::text
                                                            )
                                                        )
                                                        OR (
                                                            (x.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                                        )
                                                    )
                                                    OR (
                                                        (x.acct_hier_shrt_desc)::text = ('SA'::character varying)::text
                                                    )
                                                )
                                                OR (
                                                    (x.acct_hier_shrt_desc)::text = ('TLO'::character varying)::text
                                                )
                                            )
                                            OR (
                                                (x.acct_hier_shrt_desc)::text = ('VGF'::character varying)::text
                                            )
                                        )
                                        OR (
                                            (x.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (x.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                    )
                                )
                                OR (
                                    (x.acct_hier_shrt_desc)::text = ('GC'::character varying)::text
                                )
                            )
                            OR (
                                (x.acct_hier_shrt_desc)::text = ('NGC'::character varying)::text
                            )
                        )
                        OR (
                            (x.acct_hier_shrt_desc)::text = ('CTD'::character varying)::text
                        )
                    )
                    OR (
                        (x.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                    )
                )
                AND (
                    (x.category)::text = ('BP'::character varying)::text
                )
            )
            AND (
                (
                    (
                        (x.ctry_key)::text = ('HK'::character varying)::text
                    )
                    OR (
                        (x.ctry_key)::text = ('KR'::character varying)::text
                    )
                )
                OR (
                    (x.ctry_key)::text = ('TW'::character varying)::text
                )
            )
        )
),
cte3 as (
    SELECT y.obj_crncy,
        y.vers,
        y.fisc_yr,
        y.fisc_yr_per,
        y.category,
        y.freq,
        y.matl_num,
        y.sls_org,
        y.dstn_chnl,
        y.ctry_key,
        y.cust_num,
        y.co_cd,
        y.acct_hier_shrt_desc,
        y.acct_num,
        y.amt_obj_crcy AS plan_amt
    FROM edw_copa_plan_fact y
    WHERE (
            (
                (
                    (
                        (y.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                    )
                    OR (
                        (y.acct_hier_shrt_desc)::text = ('NTS'::character varying)::text
                    )
                )
                AND (
                    (y.category)::text = ('BP'::character varying)::text
                )
            )
            AND (
                (
                    (
                        (y.ctry_key)::text = ('HK'::character varying)::text
                    )
                    OR (
                        (y.ctry_key)::text = ('KR'::character varying)::text
                    )
                )
                OR (
                    (y.ctry_key)::text = ('TW'::character varying)::text
                )
            )
        )
),
cte4 as (
    SELECT p.obj_crncy,
        p.vers,
        p.fisc_yr,
        p.fisc_yr_per,
        p.category,
        p.freq,
        p.matl_num,
        p.sls_org,
        p.dstn_chnl,
        p.ctry_key,
        p.cust_num,
        p.co_cd,
        p.acct_hier_shrt_desc,
        p.acct_num,
        p.plan_amt
    FROM (
            SELECT x.obj_crncy,
                x.vers,
                x.fisc_yr,
                x.fisc_yr_per,
                'LE'::character varying AS category,
                'QTD'::character varying AS freq,
                x.matl_num,
                x.sls_org,
                x.dstn_chnl,
                x.ctry_key,
                x.cust_num,
                x.co_cd,
                'TP'::character varying AS acct_hier_shrt_desc,
                x.acct_num,
                CASE
                    WHEN (
                        (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                    ) THEN (
                        (- ((1)::numeric)::numeric(18, 0)) * x.amt_obj_crcy
                    )
                    ELSE x.amt_obj_crcy
                END AS plan_amt
            FROM edw_copa_plan_fact x
            WHERE (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                (
                                                                                    (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                                                                )
                                                                                OR (
                                                                                    (x.acct_hier_shrt_desc)::text = ('CD'::character varying)::text
                                                                                )
                                                                            )
                                                                            OR (
                                                                                (x.acct_hier_shrt_desc)::text = ('LF'::character varying)::text
                                                                            )
                                                                        )
                                                                        OR (
                                                                            (x.acct_hier_shrt_desc)::text = ('PSO'::character varying)::text
                                                                        )
                                                                    )
                                                                    OR (
                                                                        (x.acct_hier_shrt_desc)::text = ('PMA'::character varying)::text
                                                                    )
                                                                )
                                                                OR (
                                                                    (x.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                                                )
                                                            )
                                                            OR (
                                                                (x.acct_hier_shrt_desc)::text = ('SA'::character varying)::text
                                                            )
                                                        )
                                                        OR (
                                                            (x.acct_hier_shrt_desc)::text = ('TLO'::character varying)::text
                                                        )
                                                    )
                                                    OR (
                                                        (x.acct_hier_shrt_desc)::text = ('VGF'::character varying)::text
                                                    )
                                                )
                                                OR (
                                                    (x.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                                )
                                            )
                                            OR (
                                                (x.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                            )
                                        )
                                        OR (
                                            (x.acct_hier_shrt_desc)::text = ('GC'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (x.acct_hier_shrt_desc)::text = ('NGC'::character varying)::text
                                    )
                                )
                                OR (
                                    (x.acct_hier_shrt_desc)::text = ('CTD'::character varying)::text
                                )
                            )
                            OR (
                                (x.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                            )
                        )
                        AND (
                            (
                                (
                                    (
                                        (x.category)::text = ('MU'::character varying)::text
                                    )
                                    OR (
                                        (x.category)::text = ('JU'::character varying)::text
                                    )
                                )
                                OR (
                                    (x.category)::text = ('SU'::character varying)::text
                                )
                            )
                            OR (
                                (x.category)::text = ('NU'::character varying)::text
                            )
                        )
                    )
                    AND (
                        (
                            (
                                (x.ctry_key)::text = ('HK'::character varying)::text
                            )
                            OR (
                                (x.ctry_key)::text = ('KR'::character varying)::text
                            )
                        )
                        OR (
                            (x.ctry_key)::text = ('TW'::character varying)::text
                        )
                    )
                )
            UNION ALL
            SELECT y.obj_crncy,
                y.vers,
                y.fisc_yr,
                y.fisc_yr_per,
                'LE'::character varying AS category,
                'QTD'::character varying AS freq,
                y.matl_num,
                y.sls_org,
                y.dstn_chnl,
                y.ctry_key,
                y.cust_num,
                y.co_cd,
                y.acct_hier_shrt_desc,
                y.acct_num,
                y.amt_obj_crcy AS plan_amt
            FROM edw_copa_plan_fact y
            WHERE (
                    (
                        (
                            (
                                (y.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                            )
                            OR (
                                (y.acct_hier_shrt_desc)::text = ('NTS'::character varying)::text
                            )
                        )
                        AND (
                            (
                                (
                                    (
                                        (y.category)::text = ('MU'::character varying)::text
                                    )
                                    OR (
                                        (y.category)::text = ('JU'::character varying)::text
                                    )
                                )
                                OR (
                                    (y.category)::text = ('SU'::character varying)::text
                                )
                            )
                            OR (
                                (y.category)::text = ('NU'::character varying)::text
                            )
                        )
                    )
                    AND (
                        (
                            (
                                (y.ctry_key)::text = ('HK'::character varying)::text
                            )
                            OR (
                                (y.ctry_key)::text = ('KR'::character varying)::text
                            )
                        )
                        OR (
                            (y.ctry_key)::text = ('TW'::character varying)::text
                        )
                    )
                )
        ) p
),
a as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
),
i as (
    SELECT edw_material_sales_dim.sls_org,
        edw_material_sales_dim.dstr_chnl,
        edw_material_sales_dim.matl_num AS matl,
        edw_material_sales_dim.ean_num
    FROM edw_material_sales_dim
    WHERE (
            (edw_material_sales_dim.ean_num)::text <> (''::character varying)::text
        )
),
d as (
    SELECT edw_company_dim.co_cd,
        edw_company_dim.ctry_nm,
        edw_company_dim.ctry_key,
        edw_company_dim.company_nm
    FROM edw_company_dim
    WHERE (
            (
                (
                    (edw_company_dim.ctry_key)::text = ('TW'::character varying)::text
                )
                OR (
                    (edw_company_dim.ctry_key)::text = ('HK'::character varying)::text
                )
            )
            OR (
                (edw_company_dim.ctry_key)::text = ('KR'::character varying)::text
            )
        )
),
cbd as (
    SELECT DISTINCT edw_customer_base_dim.cust_num,
        edw_customer_base_dim.cust_nm
    FROM edw_customer_base_dim
),
md as (
    SELECT DISTINCT edw_material_dim.matl_num,
        edw_material_dim.matl_desc
    FROM edw_material_dim
),
final as (
    SELECT d.ctry_nm,
    a.cust_num,
    cbd.cust_nm,
    a.matl_num,
    md.matl_desc,
    (j.prod_hier_l4)::character varying(100) AS mega_brnd_desc,
    (j.prod_hier_l4)::character varying(100) AS brnd_desc,
    i.ean_num,
    j.prod_hier_l1,
    j.prod_hier_l2,
    j.prod_hier_l3,
    j.prod_hier_l4,
    j.prod_hier_l5,
    j.prod_hier_l6,
    j.prod_hier_l7,
    j.prod_hier_l8,
    j.prod_hier_l9,
    a.fisc_yr,
    a.fisc_yr_per,
    a.category,
    a.acct_hier_shrt_desc,
    CASE
        WHEN (
            (a.acct_hier_shrt_desc)::text = ('TP'::character varying)::text
        ) THEN (
            sum(a.plan_amt) * (- ((1)::numeric)::numeric(18, 0))
        )
        ELSE sum(a.plan_amt)
    END AS plan_val,
    g.from_crncy,
    g.to_crncy,
    g.ex_rt
FROM a
    LEFT JOIN v_intrm_crncy_exch g ON (((a.obj_crncy)::text = (g.from_crncy)::text))
    LEFT JOIN i ON (
        (
            (
                (
                    ltrim(
                        (a.matl_num)::text,
                        ((0)::character varying)::text
                    ) = ltrim((i.matl)::text, ((0)::character varying)::text)
                )
                AND (
                    (COALESCE(a.sls_org, '#'::character varying))::text = (COALESCE(i.sls_org, '#'::character varying))::text
                )
            )
            AND (
                (COALESCE(a.dstn_chnl, '#'::character varying))::text = (COALESCE(i.dstr_chnl, '#'::character varying))::text
            )
        )
    )
    LEFT JOIN edw_product_attr_dim j ON (
        (
            ((i.ean_num)::text = (j.aw_remote_key)::text)
            AND ((a.ctry_key)::text = (j.cntry)::text)
        )
    )
    JOIN d ON (((a.co_cd)::text = (d.co_cd)::text))
    LEFT JOIN cbd ON (
        (
            ltrim(
                (a.cust_num)::text,
                ((0)::character varying)::text
            ) = ltrim(
                (cbd.cust_num)::text,
                ((0)::character varying)::text
            )
        )
    )
    LEFT JOIN md ON (
        (
            ltrim(
                (a.matl_num)::text,
                ((0)::character varying)::text
            ) = ltrim(
                (md.matl_num)::text,
                ((0)::character varying)::text
            )
        )
    )
GROUP BY d.ctry_nm,
    i.ean_num,
    j.prod_hier_l1,
    j.prod_hier_l2,
    j.prod_hier_l3,
    j.prod_hier_l4,
    j.prod_hier_l5,
    j.prod_hier_l6,
    j.prod_hier_l7,
    j.prod_hier_l8,
    j.prod_hier_l9,
    a.fisc_yr,
    a.fisc_yr_per,
    a.category,
    a.obj_crncy,
    g.from_crncy,
    g.to_crncy,
    g.ex_rt,
    a.acct_hier_shrt_desc,
    a.cust_num,
    cbd.cust_nm,
    a.matl_num,
    md.matl_desc
)
select * from final