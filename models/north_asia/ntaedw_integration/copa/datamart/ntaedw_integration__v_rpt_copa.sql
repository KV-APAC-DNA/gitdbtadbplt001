with v_intrm_copa_trans as 
(
    select * from ntaedw_integration.v_intrm_copa_trans
),
final as
(
    SELECT 
        ctry_nm AS cntry_key,
        acct_num,
        cust_num,
        channel,
        med_desc AS prft_ctr,
        ltrim(
            (matl_num)::text,
            ((0)::character varying)::text
        ) AS matl,
        COALESCE(
            matl_desc,
            'Not Available'::character varying
        ) AS matl_desc,
        ean_num,
        COALESCE(
            prod_hier_lvl1,
            'Not Available'::character varying
        ) AS prod_hier_lvl1,
        COALESCE(
            prod_hier_lvl2,
            'Not Available'::character varying
        ) AS prod_hier_lvl2,
        COALESCE(
            prod_hier_lvl3,
            'Not Available'::character varying
        ) AS prod_hier_lvl3,
        COALESCE(
            prod_hier_lvl4,
            'Not Available'::character varying
        ) AS mega_brnd_desc,
        COALESCE(
            prod_hier_lvl4,
            'Not Available'::character varying
        ) AS brnd_desc,
        COALESCE(
            prod_minor,
            'Not Available'::character varying
        ) AS prod_minor,
        COALESCE(
            sls_grp,
            'Not Available'::character varying
        ) AS sls_grp,
        COALESCE(
            sls_grp_desc,
            'Not Available'::character varying
        ) AS sls_grp_desc,
        COALESCE(
            sls_ofc,
            'Not Available'::character varying
        ) AS sls_ofc,
        COALESCE(
            sls_ofc_desc,
            'Not Available'::character varying
        ) AS sls_ofc_desc,
        COALESCE(
            prod_hier_lvl5,
            'Not Available'::character varying
        ) AS category_1,
        COALESCE(
            prod_hier_lvl6,
            'Not Available'::character varying
        ) AS categroy_2,
        COALESCE(
            prod_hier_lvl7,
            'Not Available'::character varying
        ) AS platform_ca,
        COALESCE(
            prod_hier_lvl8,
            'Not Available'::character varying
        ) AS prod_hier_lvl8,
        COALESCE(
            prod_hier_lvl9,
            'Not Available'::character varying
        ) AS prod_hier_lvl9,
        sum(amt_obj_crncy) AS amt_obj_crncy,
        obj_crncy_co_obj,
        COALESCE(
            edw_cust_nm,
            'Not Available'::character varying
        ) AS edw_cust_nm,
        from_crncy,
        to_crncy,
        ex_rt_typ,
        ex_rt,
        COALESCE(
            prod_hier_lvl3,
            'Others'::character varying
        ) AS brand_group,
        try_to_date(
            (
                (
                    "substring"(
                        (
                            (fisc_yr_per)::character varying
                        )::text,
                        6,
                        8
                    ) || ('01'::character varying)::text
                ) || "substring"(
                    (
                        (fisc_yr_per)::character varying
                    )::text,
                    1,
                    4
                )
            ),
            ('MMDDYYYY'::character varying)::text
        ) AS fisc_day,
        fisc_yr_per,
        caln_day,
        caln_yr_mo,
        qty,
        uom,
        acct_hier_desc,
        acct_hier_shrt_desc,
        ctry_key AS cntry_cd,
        company_nm,
        store_type,
        plnt
    FROM v_intrm_copa_trans
    WHERE 
        (
            (
                (fisc_yr_per)::character varying
            )::text >= (
                (
                    (
                        (
                            (
                                (
                                    date_part(
                                        year,
                                        current_timestamp()::timestamp without time zone
                                    ) - (2)::double precision
                                )
                            )::character varying
                        )::text || ((0)::character varying)::text
                    ) || ((0)::character varying)::text
                ) || ((1)::character varying)::text
            )
        )
    GROUP BY acct_num,
        matl_num,
        matl_desc,
        ean_num,
        obj_crncy_co_obj,
        from_crncy,
        to_crncy,
        ctry_key,
        ctry_nm,
        cust_num,
        prod_minor,
        sls_grp,
        sls_grp_desc,
        sls_ofc,
        sls_ofc_desc,
        edw_cust_nm,
        channel,
        med_desc,
        prod_hier_lvl1,
        prod_hier_lvl2,
        prod_hier_lvl4,
        ex_rt_typ,
        ex_rt,
        company_nm,
        acct_hier_desc,
        acct_hier_shrt_desc,
        prod_hier_lvl3,
        prod_hier_lvl5,
        prod_hier_lvl6,
        prod_hier_lvl7,
        prod_hier_lvl8,
        prod_hier_lvl9,
        fisc_yr_per,
        caln_day,
        caln_yr_mo,
        qty,
        uom,
        store_type,
        plnt
)
select * from final