with edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_sales_dim') }}
),
edw_code_descriptions as
(
    select * from {{ ref('aspedw_integration__edw_code_descriptions') }}
),
vw_iri_scan_sales_analysis as
(
    select * from {{ ref('pcfedw_integration__vw_iri_scan_sales_analysis') }}
),
itg_mds_pacific_prod_mapping_cwh as
(
    select * from {{ ref('pcfitg_integration__itg_mds_pacific_prod_mapping_cwh') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_list_price as
(
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
itg_parameter_reg_inventory as
(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
), 
edw_metcash_ind_grocery_fact as
(
    select * from {{ ref('pcfedw_integration__edw_metcash_ind_grocery_fact') }}
),
edw_perenso_prod_dim as
(
    select * from {{ ref('pcfedw_integration__edw_perenso_prod_dim') }}
),
itg_customer_sellout as
(
    select * from {{ ref('pcfitg_integration__itg_customer_sellout') }}
),
final as
(
SELECT b.channel_desc,
    a.sap_prnt_cust_key,
    a.sap_prnt_cust_desc,
    a."month",
    a.matl_id,
    a.so_qty,
    a.so_value
FROM (
        (
            (
                SELECT cust.sap_prnt_cust_key,
                    (upper(TRIM((cust.sap_prnt_cust_desc)::text)))::character varying AS sap_prnt_cust_desc,
                    a."month",
                    a.matl_id,
                    a.so_qty,
                    ((a.so_qty * lp.amount))::numeric(16, 4) AS so_value
                FROM (
                        SELECT DISTINCT ecsd.prnt_cust_key AS sap_prnt_cust_key,
                            cddes_pck.code_desc AS sap_prnt_cust_desc
                        FROM edw_customer_base_dim ecbd,
                            (
                                edw_customer_sales_dim ecsd
                                LEFT JOIN edw_code_descriptions cddes_pck ON (
                                    (
                                        (
                                            (cddes_pck.code)::text = (ecsd.prnt_cust_key)::text
                                        )
                                        AND (
                                            (cddes_pck.code_type)::text = ('Parent Customer Key'::character varying)::text
                                        )
                                    )
                                )
                            )
                        WHERE (
                                ((ecsd.cust_num)::text = (ecbd.cust_num)::text)
                                AND (
                                    (
                                        (
                                            (
                                                (
                                                    (ecsd.sls_org)::text = ('3300'::character varying)::text
                                                )
                                                OR (
                                                    (ecsd.sls_org)::text = ('330B'::character varying)::text
                                                )
                                            )
                                            OR (
                                                (ecsd.sls_org)::text = ('330H'::character varying)::text
                                            )
                                        )
                                        OR (
                                            (ecsd.sls_org)::text = ('3410'::character varying)::text
                                        )
                                    )
                                    OR (
                                        (ecsd.sls_org)::text = ('341B'::character varying)::text
                                    )
                                )
                            )
                    ) cust,
                    (
                        (
                            SELECT vw_iri_scan_sales_analysis."month",
                                vw_iri_scan_sales_analysis.sales_grp_nm,
                                vw_iri_scan_sales_analysis.matl_id,
                                (vw_iri_scan_sales_analysis.so_qty)::numeric(10, 4) AS so_qty
                            FROM (
                                    SELECT (iri1.jj_mnth_id)::character varying AS "month",
                                        iri1.sales_grp_nm,
                                        (COALESCE(iri1.matl_id, iri1.lst_sku))::character varying AS matl_id,
                                        (iri1.lst_sku)::character varying AS lst_sku,
                                        (iri1.scan_units)::numeric(10, 4) AS so_qty
                                    FROM vw_iri_scan_sales_analysis iri1
                                    WHERE (
                                            (
                                                upper((iri1.sales_grp_nm)::text) = ('COLES'::character varying)::text
                                            )
                                            OR (
                                                upper((iri1.sales_grp_nm)::text) = ('WOOLWORTHS'::character varying)::text
                                            )
                                        )
                                    UNION ALL
                                    SELECT (iri2.jj_mnth_id)::character varying AS "month",
                                        iri2.sales_grp_nm,
                                        (COALESCE(iri2.matl_id, iri2.lst_sku))::character varying AS matl_id,
                                        (iri2.lst_sku)::character varying AS lst_sku,
                                        (iri2.scan_units)::numeric(10, 4) AS so_qty
                                    FROM vw_iri_scan_sales_analysis iri2,
                                        itg_mds_pacific_prod_mapping_cwh cwhprod
                                    WHERE (
                                            (
                                                ltrim(
                                                    COALESCE(iri2.matl_id, iri2.lst_sku),
                                                    ('0'::character varying)::text
                                                ) = ltrim(
                                                    ((cwhprod.matl_num)::character varying)::text,
                                                    ('0'::character varying)::text
                                                )
                                            )
                                            AND (
                                                (iri2.ac_nielsencode)::text = ('MCG'::character varying)::text
                                            )
                                        )
                                ) vw_iri_scan_sales_analysis
                            WHERE (
                                    vw_iri_scan_sales_analysis."month" IN (
                                        SELECT (derived_table2.jj_mnth_id)::character varying AS jj_mnth_id
                                        FROM (
                                                SELECT a.jj_mnth_id,
                                                    a.jj_mnth_wk,
                                                    b.jj_max_wk_no,
                                                    (b.jj_max_wk_no - a.jj_mnth_wk) AS diff
                                                FROM (
                                                        (
                                                            SELECT derived_table1.jj_mnth_id,
                                                                "max"(derived_table1.jj_mnth_wk) AS jj_mnth_wk
                                                            FROM (
                                                                    SELECT DISTINCT vw_iri_scan_sales_analysis.jj_mnth_id,
                                                                        vw_iri_scan_sales_analysis.jj_mnth_wk,
                                                                        vw_iri_scan_sales_analysis.wk_end_dt
                                                                    FROM vw_iri_scan_sales_analysis
                                                                    WHERE (
                                                                            (
                                                                                upper(
                                                                                    TRIM((vw_iri_scan_sales_analysis.sales_grp_nm)::text)
                                                                                ) = ('COLES'::character varying)::text
                                                                            )
                                                                            OR (
                                                                                upper(
                                                                                    TRIM((vw_iri_scan_sales_analysis.sales_grp_nm)::text)
                                                                                ) = ('WOOLWORTHS'::character varying)::text
                                                                            )
                                                                        )
                                                                    UNION ALL
                                                                    SELECT DISTINCT vw_iri_scan_sales_analysis.jj_mnth_id,
                                                                        vw_iri_scan_sales_analysis.jj_mnth_wk,
                                                                        vw_iri_scan_sales_analysis.wk_end_dt
                                                                    FROM vw_iri_scan_sales_analysis
                                                                    WHERE (
                                                                            upper(
                                                                                TRIM(
                                                                                    (vw_iri_scan_sales_analysis.ac_nielsencode)::text
                                                                                )
                                                                            ) = ('MCG'::character varying)::text
                                                                        )
                                                                ) derived_table1
                                                            GROUP BY derived_table1.jj_mnth_id
                                                        ) a
                                                        JOIN (
                                                            SELECT edw_vw_os_time_dim.mnth_id,
                                                                "max"(edw_vw_os_time_dim.mnth_wk_no) AS jj_max_wk_no
                                                            FROM edw_vw_os_time_dim
                                                            GROUP BY edw_vw_os_time_dim.mnth_id
                                                        ) b ON (
                                                            (
                                                                ((a.jj_mnth_id)::character varying)::text = b.mnth_id
                                                            )
                                                        )
                                                    )
                                            ) derived_table2
                                        WHERE (derived_table2.diff = 0)
                                    )
                                )
                        ) a
                        LEFT JOIN (
                            SELECT lp.material,
                                lp.list_price,
                                b.parameter_value,
                                (
                                    lp.list_price * (b.parameter_value)::numeric(10, 4)
                                ) AS amount
                            FROM (
                                    SELECT ltrim(
                                            (edw_list_price.material)::text,
                                            ((0)::character varying)::text
                                        ) AS material,
                                        edw_list_price.amount AS list_price,
                                        row_number() OVER(
                                            PARTITION BY ltrim(
                                                (edw_list_price.material)::text,
                                                ((0)::character varying)::text
                                            )
                                            ORDER BY to_date(
                                                    (edw_list_price.valid_to)::text,
                                                    ('YYYYMMDD'::character varying)::text
                                                ) DESC,
                                                to_date(
                                                    (edw_list_price.dt_from)::text,
                                                    ('YYYYMMDD'::character varying)::text
                                                ) DESC
                                        ) AS rn
                                    FROM edw_list_price
                                    WHERE (
                                            (edw_list_price.sls_org)::text = ('3300'::character varying)::text
                                        )
                                ) lp,
                                itg_parameter_reg_inventory b
                            WHERE (
                                    (
                                        (lp.rn = 1)
                                        AND (
                                            (b.country_name)::text = ('AUSTRALIA'::character varying)::text
                                        )
                                    )
                                    AND (
                                        (b.parameter_name)::text = ('listprice'::character varying)::text
                                    )
                                )
                        ) lp ON (
                            (
                                ltrim(
                                    (a.matl_id)::text,
                                    ((0)::character varying)::text
                                ) = ltrim(lp.material, ((0)::character varying)::text)
                            )
                        )
                    )
                WHERE (
                        upper(TRIM((cust.sap_prnt_cust_desc)::text)) = CASE
                            WHEN (
                                (
                                    upper(TRIM((a.sales_grp_nm)::text)) = ('SIGMA'::character varying)::text
                                )
                                OR (
                                    upper(TRIM((a.sales_grp_nm)::text)) = ('API'::character varying)::text
                                )
                            ) THEN ('CHEMIST WAREHOUSE'::character varying)::text
                            ELSE upper(TRIM((a.sales_grp_nm)::text))
                        END
                    )
                UNION ALL
                SELECT derived_table1.sap_prnt_cust_key,
                    (
                        upper(TRIM((derived_table1.sap_prnt_cust_desc)::text))
                    )::character varying AS sap_prnt_cust_desc,
                    derived_table1."month",
                    derived_table1.matl_num,
                    derived_table1.gross_units AS so_qty,
                    (
                        (
                            derived_table1.gross_units * derived_table1.amount
                        )
                    )::numeric(16, 4) AS so_value
                FROM (
                        SELECT a.matl_num,
                            a.ean,
                            a."month",
                            a.gross_units,
                            lp.material,
                            lp.list_price,
                            lp.parameter_value,
                            lp.amount,
                            cust.sap_prnt_cust_key,
                            cust.sap_prnt_cust_desc
                        FROM (
                                SELECT DISTINCT ecsd.prnt_cust_key AS sap_prnt_cust_key,
                                    cddes_pck.code_desc AS sap_prnt_cust_desc
                                FROM edw_customer_base_dim ecbd,
                                    (
                                        edw_customer_sales_dim ecsd
                                        LEFT JOIN edw_code_descriptions cddes_pck ON (
                                            (
                                                (
                                                    (cddes_pck.code)::text = (ecsd.prnt_cust_key)::text
                                                )
                                                AND (
                                                    (cddes_pck.code_type)::text = ('Parent Customer Key'::character varying)::text
                                                )
                                            )
                                        )
                                    )
                                WHERE (
                                        ((ecsd.cust_num)::text = (ecbd.cust_num)::text)
                                        AND (
                                            (
                                                (
                                                    (
                                                        (
                                                            (ecsd.sls_org)::text = ('3300'::character varying)::text
                                                        )
                                                        OR (
                                                            (ecsd.sls_org)::text = ('330B'::character varying)::text
                                                        )
                                                    )
                                                    OR (
                                                        (ecsd.sls_org)::text = ('330H'::character varying)::text
                                                    )
                                                )
                                                OR (
                                                    (ecsd.sls_org)::text = ('3410'::character varying)::text
                                                )
                                            )
                                            OR (
                                                (ecsd.sls_org)::text = ('341B'::character varying)::text
                                            )
                                        )
                                    )
                            ) cust,
                            (
                                (
                                    SELECT b.prod_id AS matl_num,
                                        b.prod_ean AS ean,
                                        a.month_number AS "month",
                                        (a.gross_units)::numeric(10, 4) AS gross_units
                                    FROM (
                                            edw_metcash_ind_grocery_fact a
                                            LEFT JOIN (
                                                SELECT edw_perenso_prod_dim.prod_key,
                                                    edw_perenso_prod_dim.prod_desc,
                                                    edw_perenso_prod_dim.prod_id,
                                                    edw_perenso_prod_dim.prod_ean,
                                                    edw_perenso_prod_dim.prod_jj_franchise,
                                                    edw_perenso_prod_dim.prod_jj_category,
                                                    edw_perenso_prod_dim.prod_jj_brand,
                                                    edw_perenso_prod_dim.prod_sap_franchise,
                                                    edw_perenso_prod_dim.prod_sap_profit_centre,
                                                    edw_perenso_prod_dim.prod_sap_product_major,
                                                    edw_perenso_prod_dim.prod_grocery_franchise,
                                                    edw_perenso_prod_dim.prod_grocery_category,
                                                    edw_perenso_prod_dim.prod_grocery_brand,
                                                    edw_perenso_prod_dim.prod_active_nz_pharma,
                                                    edw_perenso_prod_dim.prod_active_au_grocery,
                                                    edw_perenso_prod_dim.prod_active_metcash,
                                                    edw_perenso_prod_dim.prod_active_nz_grocery,
                                                    edw_perenso_prod_dim.prod_active_au_pharma,
                                                    edw_perenso_prod_dim.prod_pbs,
                                                    edw_perenso_prod_dim.prod_ims_brand,
                                                    edw_perenso_prod_dim.prod_nz_code,
                                                    edw_perenso_prod_dim.prod_metcash_code,
                                                    edw_perenso_prod_dim.prod_old_id,
                                                    edw_perenso_prod_dim.prod_old_ean,
                                                    edw_perenso_prod_dim.prod_tax,
                                                    edw_perenso_prod_dim.prod_bwp_aud,
                                                    edw_perenso_prod_dim.prod_bwp_nzd
                                                FROM edw_perenso_prod_dim
                                                WHERE (
                                                        (edw_perenso_prod_dim.prod_metcash_code)::text <> ('NOT ASSIGNED'::character varying)::text
                                                    )
                                            ) b ON (
                                                (
                                                    ltrim(
                                                        (a.product_id)::text,
                                                        ((0)::character varying)::text
                                                    ) = ltrim(
                                                        (b.prod_metcash_code)::text,
                                                        ((0)::character varying)::text
                                                    )
                                                )
                                            )
                                        )
                                ) a
                                LEFT JOIN (
                                    SELECT lp.material,
                                        lp.list_price,
                                        b.parameter_value,
                                        (
                                            lp.list_price * (b.parameter_value)::numeric(10, 4)
                                        ) AS amount
                                    FROM (
                                            SELECT ltrim(
                                                    (edw_list_price.material)::text,
                                                    ((0)::character varying)::text
                                                ) AS material,
                                                edw_list_price.amount AS list_price,
                                                row_number() OVER(
                                                    PARTITION BY ltrim(
                                                        (edw_list_price.material)::text,
                                                        ((0)::character varying)::text
                                                    )
                                                    ORDER BY to_date(
                                                            (edw_list_price.valid_to)::text,
                                                            ('YYYYMMDD'::character varying)::text
                                                        ) DESC,
                                                        to_date(
                                                            (edw_list_price.dt_from)::text,
                                                            ('YYYYMMDD'::character varying)::text
                                                        ) DESC
                                                ) AS rn
                                            FROM edw_list_price
                                            WHERE (
                                                    (edw_list_price.sls_org)::text = ('3300'::character varying)::text
                                                )
                                        ) lp,
                                        itg_parameter_reg_inventory b
                                    WHERE (
                                            (
                                                (lp.rn = 1)
                                                AND (
                                                    (b.country_name)::text = ('AUSTRALIA'::character varying)::text
                                                )
                                            )
                                            AND (
                                                (b.parameter_name)::text = ('listprice'::character varying)::text
                                            )
                                        )
                                ) lp ON (
                                    (
                                        ltrim(
                                            (a.matl_num)::text,
                                            ((0)::character varying)::text
                                        ) = ltrim(lp.material, ((0)::character varying)::text)
                                    )
                                )
                            )
                        WHERE (
                                upper(TRIM((cust.sap_prnt_cust_desc)::text)) = (
                                    (
                                        SELECT itg_parameter_reg_inventory.parameter_value
                                        FROM itg_parameter_reg_inventory
                                        WHERE (
                                                (itg_parameter_reg_inventory.parameter_name)::text = ('parent_desc_IG'::character varying)::text
                                            )
                                    )
                                )::text
                            )
                    ) derived_table1
            )
            UNION ALL
            SELECT itg_customer_sellout.sap_parent_customer_key,
                (
                    upper(
                        TRIM(
                            (itg_customer_sellout.sap_parent_customer_desc)::text
                        )
                    )
                )::character varying AS sap_parent_customer_desc,
                (
                    to_char(
                        (
                            (
                                (itg_customer_sellout.so_date)::timestamp without time zone
                            )
                        )::timestamp without time zone,
                        ('YYYYMM'::character varying)::text
                    )
                )::character varying AS "month",
                itg_customer_sellout.matl_num,
                (itg_customer_sellout.so_qty)::numeric(10, 4) AS so_qty,
                (
                    (
                        itg_customer_sellout.so_qty * itg_customer_sellout.std_cost
                    )
                )::numeric(16, 4) AS so_val
            FROM itg_customer_sellout
        ) a
        LEFT JOIN (
            SELECT DISTINCT itg_parameter_reg_inventory.parameter_value AS channel_desc,
                itg_parameter_reg_inventory.parameter_name
            FROM itg_parameter_reg_inventory
            WHERE (
                    (itg_parameter_reg_inventory.country_name)::text = ('AUSTRALIA'::character varying)::text
                )
        ) b ON (
            (
                upper(TRIM((a.sap_prnt_cust_desc)::text)) = upper(TRIM((b.parameter_name)::text))
            )
        )
    )
)
select * from final