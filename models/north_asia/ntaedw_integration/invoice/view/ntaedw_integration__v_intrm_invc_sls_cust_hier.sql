with edw_invoice_fact as (
    select * from snapaspedw_integration.edw_invoice_fact
),
edw_material_dim as (
    select * from snapaspedw_integration.edw_material_dim
),
edw_company_dim as (
    select * from snapaspedw_integration.edw_company_dim
),
edw_material_sales_dim as (
    select * from snapaspedw_integration.edw_material_sales_dim
),
edw_customer_base_dim as (
    select * from snapaspedw_integration.edw_customer_base_dim
),
edw_product_attr_dim as (
    select * from snapaspedw_integration.edw_product_attr_dim
),
edw_customer_sales_dim as (
    select * from snapaspedw_integration.edw_customer_sales_dim
),
edw_customer_attr_hier_dim as (
    select * from snapntaedw_integration.edw_customer_attr_hier_dim
),
v_intrm_crncy_exch as (
    select * from snapntaedw_integration.v_intrm_crncy_exch
),
c as (
    SELECT DISTINCT edw_material_dim.matl_num,
        edw_material_dim.matl_desc
    FROM edw_material_dim
),
e as (
    SELECT edw_company_dim.co_cd,
        edw_company_dim.ctry_key,
        edw_company_dim.ctry_nm,
        edw_company_dim.company_nm
    FROM edw_company_dim
    WHERE (
            (
                (
                    (edw_company_dim.ctry_key)::text = ('KR'::character varying)::text
                )
                OR (
                    (edw_company_dim.ctry_key)::text = ('HK'::character varying)::text
                )
            )
            OR (
                (edw_company_dim.ctry_key)::text = ('TW'::character varying)::text
            )
        )
),
cbd as (
    SELECT DISTINCT edw_customer_base_dim.cust_num,
        edw_customer_base_dim.cust_nm
    FROM edw_customer_base_dim
),
final as (
    SELECT e.ctry_nm,
        a.cust_num,
        cbd.cust_nm,
        a.matl_num,
        c.matl_desc,
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
        a.fisc_yr,
        sum(a.net_bill_val) AS net_bill_val,
        sum(a.bill_qty_pc) AS ord_qty_pc,
        g.from_crncy,
        g.to_crncy,
        g.ex_rt_typ,
        g.ex_rt
    FROM edw_invoice_fact a
        LEFT JOIN c ON (
            (
                ltrim(
                    (a.matl_num)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (c.matl_num)::text,
                    ((0)::character varying)::text
                )
            )
        )
        JOIN e ON (((a.co_cd)::text = (e.co_cd)::text))
        LEFT JOIN v_intrm_crncy_exch g ON (((a.curr_key)::text = (g.from_crncy)::text))
        LEFT JOIN edw_material_sales_dim i ON (
            (
                (
                    (
                        ltrim(
                            (a.matl_num)::text,
                            ((0)::character varying)::text
                        ) = ltrim(
                            (i.matl_num)::text,
                            ((0)::character varying)::text
                        )
                    )
                    AND (
                        (COALESCE(a.sls_org, '#'::character varying))::text = (COALESCE(i.sls_org, '#'::character varying))::text
                    )
                )
                AND (
                    (COALESCE(a.dstr_chnl, '#'::character varying))::text = (COALESCE(i.dstr_chnl, '#'::character varying))::text
                )
            )
        )
        LEFT JOIN edw_product_attr_dim j ON (
            (
                ((i.ean_num)::text = (j.aw_remote_key)::text)
                AND ((e.ctry_key)::text = (j.cntry)::text)
            )
        )
        LEFT JOIN edw_customer_attr_hier_dim h ON (
            (
                ltrim(
                    (a.cust_num)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (h.sold_to_party)::text,
                    ((0)::character varying)::text
                )
            )
        )
        LEFT JOIN edw_customer_sales_dim b ON (
            (
                (
                    (
                        (
                            ltrim(
                                (a.cust_num)::text,
                                ((0)::character varying)::text
                            ) = ltrim(
                                (b.cust_num)::text,
                                ((0)::character varying)::text
                            )
                        )
                        AND (
                            (COALESCE(a.sls_org, '#'::character varying))::text = (COALESCE(b.sls_org, '#'::character varying))::text
                        )
                    )
                    AND (
                        (COALESCE(a.dstr_chnl, '#'::character varying))::text = (COALESCE(b.dstr_chnl, '#'::character varying))::text
                    )
                )
                AND (
                    (COALESCE(a.div, '#'::character varying))::text = (COALESCE(b.div, '#'::character varying))::text
                )
            )
        )
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
    WHERE (a.sls_doc_typ)::text <> ('ZJSL'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZMED'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZRMD'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZRSX'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZSML'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZSMX'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZRFG'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZFGD'::character varying)::text
        AND (a.sls_doc_typ)::text <> ('ZRSM'::character varying)::text
    GROUP BY e.ctry_nm,
        a.cust_num,
        cbd.cust_nm,
        a.matl_num,
        c.matl_desc,
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
        a.fisc_yr,
        g.from_crncy,
        g.to_crncy,
        g.ex_rt_typ,
        g.ex_rt
)
select * from final