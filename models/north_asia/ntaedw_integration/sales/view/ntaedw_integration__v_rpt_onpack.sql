with edw_invoice_fact as (
    select * from aspedw_integration.edw_invoice_fact
),
edw_customer_sales_dim as (
    select * from aspedw_integration.edw_customer_sales_dim
),
edw_company_dim as (
    select * from aspedw_integration.edw_company_dim
),
edw_customer_attr_flat_dim as (
    select * from aspedw_integration.edw_customer_attr_flat_dim
),
edw_material_sales_dim as (
    select * from aspedw_integration.edw_material_sales_dim
),
edw_product_attr_dim as (
    select * from aspedw_integration.edw_product_attr_dim
),
itg_sls_grp_text as (
    select * from aspitg_integration.itg_sls_grp_text
),
v_rpt_onpack_inventory as (
    select * from ntaedw_integration.v_rpt_onpack_inventory
),
edw_onpck_trgt as (
    select * from ntaedw_integration.edw_onpck_trgt
),
itg_kr_sales_store_map as (
    select * from aspitg_integration.itg_kr_sales_store_map
),
invc as (
    SELECT "substring"((a.fisc_yr)::text, 1, 4) AS fisc_yr,
        "substring"((a.fisc_yr)::text, 6, 8) AS fisc_mo,
        a.matl_num,
        b.sls_grp,
        e.ctry_key AS ctry_cd,
        e.ctry_nm,
        sum(a.bill_qty_pc) AS invoice_qty,
        h.channel,
        a.sls_org,
        a.dstr_chnl
    FROM edw_invoice_fact a
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
                        AND ((a.dstr_chnl)::text = (b.dstr_chnl)::text)
                    )
                    AND ((a.sls_org)::text = (b.sls_org)::text)
                )
                AND ((a.div)::text = (b.div)::text)
            )
        )
        LEFT JOIN edw_company_dim e ON (((a.co_cd)::text = (e.co_cd)::text))
        LEFT JOIN (
            SELECT DISTINCT edw_customer_attr_dim.sold_to_party AS sold_to_prty,
                edw_customer_attr_dim.channel
            FROM edw_customer_attr_flat_dim edw_customer_attr_dim
            WHERE (
                    (edw_customer_attr_dim.trgt_type)::text = ('flat'::character varying)::text
                )
        ) h ON (
            (
                ltrim(
                    (a.sold_to_prty)::text,
                    ((0)::character varying)::text
                ) = ltrim(
                    (h.sold_to_prty)::text,
                    ((0)::character varying)::text
                )
            )
        )
    WHERE (
            (
                (
                    (e.ctry_key)::text = ('TW'::character varying)::text
                )
                OR (
                    (e.ctry_key)::text = ('KR'::character varying)::text
                )
            )
            AND (
                (a.bill_typ)::text like ('ZF2%'::character varying)::text
            )
        )
    GROUP BY a.fisc_yr,
        a.matl_num,
        b.sls_grp,
        e.ctry_key,
        e.ctry_nm,
        h.channel,
        a.sls_org,
        a.dstr_chnl
),
mat as (
    SELECT DISTINCT edw_material_sales_dim.sls_org,
        edw_material_sales_dim.dstr_chnl,
        edw_material_sales_dim.matl_num AS matl,
        edw_material_sales_dim.ean_num,
        (edw_material_sales_dim.med_desc)::character varying(100) AS matl_desc
    FROM edw_material_sales_dim edw_material_sales_dim
    WHERE (
            (
                (
                    ltrim((edw_material_sales_dim.ean_num)::text) <> (''::character varying)::text
                )
                AND (
                    ltrim((edw_material_sales_dim.med_desc)::text) <> (''::character varying)::text
                )
            )
            AND (
                (
                    (
                        (
                            ltrim((edw_material_sales_dim.sls_org)::text) = ('3200'::character varying)::text
                        )
                        OR (
                            ltrim((edw_material_sales_dim.sls_org)::text) = ('320A'::character varying)::text
                        )
                    )
                    OR (
                        ltrim((edw_material_sales_dim.sls_org)::text) = ('320S'::character varying)::text
                    )
                )
                OR (
                    ltrim((edw_material_sales_dim.sls_org)::text) = ('321A'::character varying)::text
                )
            )
        )
),
prod as (
    SELECT DISTINCT (edw_product_attr_dim.ean)::character varying(100) AS ean_num,
        edw_product_attr_dim.cntry,
        edw_product_attr_dim.prod_hier_l1,
        edw_product_attr_dim.prod_hier_l2,
        edw_product_attr_dim.prod_hier_l3,
        (edw_product_attr_dim.prod_hier_l4)::character varying(100) AS prod_hier_l4,
        edw_product_attr_dim.prod_hier_l5,
        edw_product_attr_dim.prod_hier_l6,
        edw_product_attr_dim.prod_hier_l7,
        edw_product_attr_dim.prod_hier_l8,
        edw_product_attr_dim.prod_hier_l9
    FROM edw_product_attr_dim edw_product_attr_dim
),
sls_grp_text as (
    SELECT itg_sls_grp_text.sls_grp,
        itg_sls_grp_text.de
    FROM itg_sls_grp_text
    WHERE (
            (itg_sls_grp_text.lang_key)::text = ('E'::character varying)::text
        )
),
inv as (
    SELECT b.ctry_key AS ctry_cd,
        a.matl_no AS matl_num,
        a.fisc_yr_per,
        a.tot_stck,
        sum(a.tot_stck) OVER(
            PARTITION BY b.ctry_key,
            a.matl_no
            ORDER BY a.fisc_yr_per ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS tot_stk_qty
    FROM (
            (
                SELECT v_rpt_onpack_inventory.co_cd,
                    v_rpt_onpack_inventory.matl_no,
                    v_rpt_onpack_inventory.fisc_yr_per,
                    v_rpt_onpack_inventory.tot_stck
                from v_rpt_onpack_inventory
            ) a
            LEFT JOIN edw_company_dim b ON (((a.co_cd)::text = (b.co_cd)::text))
        )
),
final as (
    SELECT y.yr AS fisc_yr,
        y.mo AS fisc_mo,
        x.ctry_cd,
        CASE
            WHEN (x.ctry_nm IS NOT NULL) THEN x.ctry_nm
            WHEN (
                (x.ctry_nm IS NULL)
                AND (
                    (y.ctry_cd)::text = ('KR'::character varying)::text
                )
            ) THEN 'South Korea'::character varying
            WHEN (
                (x.ctry_nm IS NULL)
                AND (
                    (y.ctry_cd)::text = ('TW'::character varying)::text
                )
            ) THEN 'Taiwan'::character varying
            ELSE 'Others'::character varying
        END AS ctry_nm,
        ltrim(
            (y.matl_num)::text,
            ((0)::character varying)::text
        ) AS matl_num,
        i.matl_desc,
        j.prod_hier_l4 AS mega_brnd_desc,
        y.acct_grp AS sls_grp,
        d.de AS sls_grp_desc,
        store_map.channel,
        store_map.store_type,
        store_map.sls_ofc_desc AS sales_office,
        x.invoice_qty,
        y.trgt_qty AS target_qty,
        z.tot_stk_qty AS curr_inventory_qty,
        '#' AS bonus_desc
    from edw_onpck_trgt y
        LEFT JOIN itg_kr_sales_store_map store_map ON (
            (
                (y.acct_grp)::text = (store_map.sales_group_code)::text
            )
        )
        LEFT JOIN invc x ON (
            ltrim(
                (x.matl_num)::text,
                ((0)::character varying)::text
            ) = ltrim(
                (y.matl_num)::text,
                ((0)::character varying)::text
            )
            AND upper((x.sls_grp)::text) = upper((y.acct_grp)::text)
            AND (x.fisc_yr = (y.yr)::text)
            AND ltrim(x.fisc_mo, ((0)::character varying)::text) = ltrim((y.mo)::text, ((0)::character varying)::text)
            AND ((x.ctry_cd)::text = (y.ctry_cd)::text)
        )
        LEFT JOIN mat i ON ltrim(
            (y.matl_num)::text,
            ((0)::character varying)::text
        ) = ltrim((i.matl)::text, ((0)::character varying)::text)
        AND ((x.dstr_chnl)::text = (i.dstr_chnl)::text)
        LEFT JOIN prod j ON ((j.ean_num)::text = (i.ean_num)::text)
        AND (j.cntry)::text = ('KR'::character varying)::text
        LEFT JOIN sls_grp_text d ON (((y.acct_grp)::text = (d.sls_grp)::text))
        LEFT JOIN inv z ON ltrim(
            (z.matl_num)::text,
            ((0)::character varying)::text
        ) = ltrim(
            (y.matl_num)::text,
            ((0)::character varying)::text
        )
        AND ((z.ctry_cd)::text = (y.ctry_cd)::text)
        AND "substring"(((z.fisc_yr_per)::character varying)::text, 1, 4) = (y.yr)::text
        AND ltrim(
            "substring"(((z.fisc_yr_per)::character varying)::text, 6, 8),
            ((0)::character varying)::text
        ) = ltrim((y.mo)::text, ((0)::character varying)::text)
)
select * from final