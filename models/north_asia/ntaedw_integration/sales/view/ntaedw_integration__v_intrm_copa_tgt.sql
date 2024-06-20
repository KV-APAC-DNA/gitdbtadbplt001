with edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_customer_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_customer_base_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_company_dim as (
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
edw_customer_attr_flat_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_attr_flat_dim') }}
),
a as (
    SELECT x.matl_num,
        x.sls_org,
        x.div,
        x.sls_grp,
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
        END AS amt_obj_crncy
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
        y.sls_org,
        y.div,
        y.sls_grp,
        y.dstr_chnl,
        y.ctry_key,
        y.fisc_yr_per,
        y.obj_crncy_co_obj,
        y.caln_day,
        y.cust_num,
        y.co_cd,
        y.acct_hier_shrt_desc AS acct_hier_cd,
        y.amt_obj_crncy
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
d as (
    SELECT edw_company_dim.co_cd,
        edw_company_dim.ctry_nm,
        edw_company_dim.ctry_key,
        edw_company_dim.company_nm
    FROM edw_company_dim
    WHERE (
            (edw_company_dim.ctry_key)::text = ('TW'::character varying)::text
        )
),
h as (
    SELECT DISTINCT edw_customer_attr_flat_dim.sold_to_party AS sold_to_prty,
        edw_customer_attr_flat_dim.channel,
        edw_customer_attr_flat_dim.sls_grp
    FROM edw_customer_attr_flat_dim
    WHERE (
            (edw_customer_attr_flat_dim.trgt_type)::text = ('flat'::character varying)::text
        )
),
final as (
    SELECT a.co_cd,
    a.cust_num,
    d.ctry_nm,
    d.ctry_key,
    a.sls_grp,
    (
        CASE
            WHEN ((f.rgn)::text = ('TPE'::character varying)::text) THEN 'TPE'::character varying
            WHEN ((f.rgn)::text = ('CNT'::character varying)::text) THEN 'Central'::character varying
            WHEN ((f.rgn)::text = ('STH'::character varying)::text) THEN 'South'::character varying
            WHEN ((f.rgn)::text = ('NTH'::character varying)::text) THEN 'North'::character varying
            ELSE 'Others'::character varying
        END
    )::character varying(150) AS rgn,
    a.fisc_yr_per,
    sum(a.amt_obj_crncy) AS amt_obj_crncy,
    a.obj_crncy_co_obj,
    b.sls_grp AS cust_sls_grp,
    COALESCE(
        (h.sls_grp)::character varying(40),
        'Not Available'::character varying
    ) AS sls_grp_desc,
    b.sls_ofc AS cust_sls_ofc,
    b.sls_ofc_desc,
    COALESCE(h.channel, 'Others'::character varying) AS channel,
    f.cust_nm AS edw_cust_nm,
    a.acct_hier_cd AS acct_hier_desc,
    a.acct_hier_cd AS acct_hier_shrt_desc,
    d.company_nm
FROM a
    LEFT JOIN edw_customer_sales_dim b ON (
        ((a.cust_num)::text = (b.cust_num)::text)
        AND ((a.sls_org)::text = (b.sls_org)::text)
    )
    AND ((a.dstr_chnl)::text = (b.dstr_chnl)::text)
    AND ((a.div)::text = (b.div)::text)
    LEFT JOIN edw_customer_base_dim f ON ltrim(
        (a.cust_num)::text,
        ((0)::character varying)::text
    ) = ltrim(
        (f.cust_num)::text,
        ((0)::character varying)::text
    )
    JOIN d ON (((a.co_cd)::text = (d.co_cd)::text))
    LEFT JOIN h ON ltrim(
        (b.cust_num)::text,
        ((0)::character varying)::text
    ) = ltrim(
        (h.sold_to_prty)::text,
        ((0)::character varying)::text
    )
WHERE (a.obj_crncy_co_obj)::text = ('USD'::character varying)::text
    OR (a.obj_crncy_co_obj)::text = ('SGD'::character varying)::text
    OR (a.obj_crncy_co_obj)::text = ('HKD'::character varying)::text
    OR (a.obj_crncy_co_obj)::text = ('TWD'::character varying)::text
    OR (
        (
            (a.obj_crncy_co_obj)::text = ('KRW'::character varying)::text
        )
        AND (
            ((a.fisc_yr_per)::character varying)::text >= (
                (
                    (
                        (
                            (
                                (
                                    date_part(
                                        year,
                                        current_timestamp::timestamp without time zone
                                    ) - 2
                                )
                            )::character varying
                        )::text || ((0)::character varying)::text
                    ) || ((0)::character varying)::text
                ) || ((1)::character varying)::text
            )
        )
    )
GROUP BY a.co_cd,
    a.cust_num,
    d.ctry_nm,
    d.ctry_key,
    a.sls_grp,
    f.rgn,
    a.fisc_yr_per,
    a.obj_crncy_co_obj,
    b.sls_grp,
    (h.sls_grp)::character varying(40),
    b.sls_ofc,
    b.sls_ofc_desc,
    COALESCE(h.channel, 'Others'::character varying),
    f.cust_nm,
    d.company_nm,
    a.acct_hier_cd
)
select * from final