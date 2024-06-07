with edw_copa_trans_fact as (
    select * from snapaspedw_integration.edw_copa_trans_fact
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
v_interm_cust_hier_dim as (
    select * from snapntaedw_integration.v_interm_cust_hier_dim
),
edw_customer_sales_dim as (
    select * from snapaspedw_integration.edw_customer_sales_dim
),
edw_customer_base_dim as (
    select * from snapaspedw_integration.edw_customer_base_dim
),
a as (
    SELECT x.matl_num,
        x.div,
        x.sls_org,
        x.dstr_chnl,
        x.ctry_key,
        x.fisc_yr_per,
        x.obj_crncy_co_obj,
        x.caln_day,
        x.cust_num,
        x.co_cd,
        'TP'::character varying AS acct_hier_cd,
        CASE
            WHEN (
                (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
            ) THEN (
                (- ((1)::numeric)::numeric(18, 0)) * x.amt_obj_crncy
            )
            ELSE x.amt_obj_crncy
        END AS copa_amt
    FROM edw_copa_trans_fact x
    WHERE (x.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('CD'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('LF'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('PSO'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('PMA'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('SA'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('TLO'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('VGF'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('GC'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('NGC'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('CTD'::character varying)::text
        OR (x.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
    UNION ALL
    SELECT y.matl_num,
        y.div,
        y.sls_org,
        y.dstr_chnl,
        y.ctry_key,
        y.fisc_yr_per,
        y.obj_crncy_co_obj,
        y.caln_day,
        y.cust_num,
        y.co_cd,
        y.acct_hier_shrt_desc AS acct_hier_cd,
        y.amt_obj_crncy AS copa_amt
    FROM edw_copa_trans_fact y
    WHERE (
            (
                (y.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
            )
            OR (
                (y.acct_hier_shrt_desc)::text = ('NTS'::character varying)::text
            )
        )
),
i as (
    SELECT edw_material_sales_dim.sls_org,
        edw_material_sales_dim.dstr_chnl,
        edw_material_sales_dim.matl_num,
        edw_material_sales_dim.ean_num
    FROM edw_material_sales_dim
    WHERE (edw_material_sales_dim.ean_num)::text <> (''::character varying)::text
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
        b.sls_ofc_desc,
        h.channel,
        h.sls_grp,
        h.cust_hier_l1,
        h.cust_hier_l2,
        h.cust_hier_l3,
        h.cust_hier_l4,
        h.cust_hier_l5,
        h.store_type,
        a.fisc_yr_per,
        a.obj_crncy_co_obj,
        a.acct_hier_cd AS acct_hier_shrt_desc,
        CASE
            WHEN (
                (a.acct_hier_cd)::text = ('TP'::character varying)::text
            ) THEN (
                sum(a.copa_amt) * (- ((1)::numeric)::numeric(18, 0))
            )
            ELSE sum(a.copa_amt)
        END AS copa_val,
        g.from_crncy,
        g.to_crncy,
        g.ex_rt_typ,
        g.ex_rt,
        date_part(
            day,
            last_day(
                (
                    (
                        (
                            (
                                "substring"(((a.fisc_yr_per)::character varying)::text, 1, 4) || "substring"(((a.fisc_yr_per)::character varying)::text, 6, 7)
                            ) || ('01'::character varying)::text
                        )
                    )::date
                )::timestamp without time zone
            )
        ) AS no_of_days
    FROM a
        LEFT JOIN v_intrm_crncy_exch g ON (a.obj_crncy_co_obj)::text = (g.from_crncy)::text
        LEFT JOIN i ON ltrim(
            (a.matl_num)::text,
            ((0)::character varying)::text
        ) = ltrim(
            (i.matl_num)::text,
            ((0)::character varying)::text
        )
        AND (COALESCE(a.sls_org, '#'::character varying))::text = (COALESCE(i.sls_org, '#'::character varying))::text
        AND (COALESCE(a.dstr_chnl, '#'::character varying))::text = (COALESCE(i.dstr_chnl, '#'::character varying))::text
        LEFT JOIN edw_product_attr_dim j ON ((i.ean_num)::text = (j.aw_remote_key)::text)
        AND ((a.ctry_key)::text = (j.cntry)::text)
        JOIN d ON ((a.co_cd)::text = (d.co_cd)::text)
        LEFT JOIN v_interm_cust_hier_dim h ON ltrim(
            (a.cust_num)::text,
            ((0)::character varying)::text
        ) = ltrim(
            (h.sold_to_party)::text,
            ((0)::character varying)::text
        )
        LEFT JOIN edw_customer_sales_dim b ON ltrim(
            (a.cust_num)::text,
            ((0)::character varying)::text
        ) = ltrim(
            (b.cust_num)::text,
            ((0)::character varying)::text
        )
        AND (COALESCE(a.sls_org, '#'::character varying))::text = (COALESCE(b.sls_org, '#'::character varying))::text
        AND (COALESCE(a.dstr_chnl, '#'::character varying))::text = (COALESCE(b.dstr_chnl, '#'::character varying))::text
        AND (COALESCE(a.div, '#'::character varying))::text = (COALESCE(b.div, '#'::character varying))::text
        LEFT JOIN cbd ON ltrim(
            (a.cust_num)::text,
            ((0)::character varying)::text
        ) = ltrim(
            (cbd.cust_num)::text,
            ((0)::character varying)::text
        )
        LEFT JOIN md ON ltrim(
            (a.matl_num)::text,
            ((0)::character varying)::text
        ) = ltrim(
            (md.matl_num)::text,
            ((0)::character varying)::text
        )
    GROUP BY d.ctry_nm,
        a.cust_num,
        cbd.cust_nm,
        a.matl_num,
        md.matl_desc,
        i.ean_num,
        a.acct_hier_cd,
        j.prod_hier_l1,
        j.prod_hier_l2,
        j.prod_hier_l3,
        j.prod_hier_l4,
        j.prod_hier_l5,
        j.prod_hier_l6,
        j.prod_hier_l7,
        j.prod_hier_l8,
        j.prod_hier_l9,
        b.sls_ofc_desc,
        h.channel,
        h.sls_grp,
        h.cust_hier_l1,
        h.cust_hier_l2,
        h.cust_hier_l3,
        h.cust_hier_l4,
        h.cust_hier_l5,
        h.store_type,
        a.fisc_yr_per,
        a.obj_crncy_co_obj,
        g.from_crncy,
        g.to_crncy,
        g.ex_rt_typ,
        g.ex_rt,
        date_part(
            day,
            last_day(
                (
                    (
                        (
                            (
                                substring(((a.fisc_yr_per)::character varying)::text, 1, 4) || substring(((a.fisc_yr_per)::character varying)::text, 6, 7)
                            ) || ('01'::character varying)::text
                        )
                    )::date
                )::timestamp without time zone
            )
        )
)
select * from final