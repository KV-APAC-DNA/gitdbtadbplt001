with edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_material_dim as
(
    select * from {{ ref('pcfedw_integration__edw_material_dim') }}
),
vw_iri_scan_sales as
(
    select * from {{ ref('pcfedw_integration__vw_iri_scan_sales') }}
),
vw_customer_dim as
(
    select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
vw_sapbw_ciw_fact as
(
    select * from {{ ref('pcfedw_integration__vw_sapbw_ciw_fact') }}
),
vw_apo_parent_child_dim as
(
    select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
),
edw_product_key_attributes as
(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
rg_edw_material_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
sales as
(
    SELECT 
        bar_cd_map.time_id,
        bar_cd_map.jj_year,
        bar_cd_map.jj_qrtr,
        bar_cd_map.jj_mnth,
        bar_cd_map.jj_wk,
        bar_cd_map.jj_mnth_wk,
        bar_cd_map.jj_mnth_id,
        bar_cd_map.jj_mnth_tot,
        bar_cd_map.jj_mnth_day,
        bar_cd_map.jj_mnth_shrt,
        bar_cd_map.jj_mnth_long,
        bar_cd_map.cal_year,
        bar_cd_map.cal_qrtr,
        bar_cd_map.cal_mnth,
        bar_cd_map.cal_wk,
        bar_cd_map.cal_mnth_wk,
        bar_cd_map.cal_mnth_id,
        bar_cd_map.cal_mnth_nm,
        bar_cd_map.wk_end_dt,
        bar_cd_map.ac_attribute,
        (bar_cd_map.cust_nm)::character varying AS cust_nm,
        bar_cd_map.channel_cd,
        bar_cd_map.channel_desc,
        bar_cd_map.sales_grp_cd,
        bar_cd_map.sales_grp_desc,
        bar_cd_map.cmp_id,
        bar_cd_map.country,
        bar_cd_map.iri_market,
        bar_cd_map.ac_nielsencode,
        bar_cd_map.ac_longname,
        bar_cd_map.iri_ean,
        bar_cd_map.matl_bar_cd,
        bar_cd_map.iri_prod_desc,
        (bar_cd_map.matl_id)::character varying AS matl_id,
        bar_cd_map.matl_desc,
        bar_cd_map.brnd_cd,
        bar_cd_map.brnd_desc,
        bar_cd_map.fran_cd,
        bar_cd_map.fran_desc,
        bar_cd_map.grp_fran_cd,
        bar_cd_map.grp_fran_desc,
        bar_cd_map.prod_fran_cd,
        bar_cd_map.prod_fran_desc,
        bar_cd_map.prod_mjr_cd,
        bar_cd_map.prod_mjr_desc,
        bar_cd_map.prod_mnr_cd,
        bar_cd_map.prod_mnr_desc,
        bar_cd_map.scan_units,
        bar_cd_map.scan_sales
    FROM 
        (
            SELECT sales_derived.time_id,
                sales_derived.jj_year,
                sales_derived.jj_qrtr,
                sales_derived.jj_mnth,
                sales_derived.jj_wk,
                sales_derived.jj_mnth_wk,
                sales_derived.jj_mnth_id,
                sales_derived.jj_mnth_tot,
                sales_derived.jj_mnth_day,
                sales_derived.jj_mnth_shrt,
                sales_derived.jj_mnth_long,
                sales_derived.cal_year,
                sales_derived.cal_qrtr,
                sales_derived.cal_mnth,
                sales_derived.cal_wk,
                sales_derived.cal_mnth_wk,
                sales_derived.cal_mnth_id,
                sales_derived.cal_mnth_nm,
                sales_derived.wk_end_dt,
                sales_derived.ac_code AS ac_attribute,
                vcd.cust_nm,
                vcd.channel_cd,
                vcd.channel_desc,
                sales_derived.sales_grp_cd,
                sales_derived.sales_grp_desc,
                vcd.cmp_id,
                vcd.country,
                sales_derived.iri_market,
                sales_derived.ac_nielsencode,
                sales_derived.ac_longname,
                sales_derived.iri_ean,
                sales_derived.matl_bar_cd,
                sales_derived.iri_prod_desc,
                ltrim(
                    (vmd.matl_id)::text,
                    ('0'::character varying)::text
                ) AS matl_id,
                vmd.matl_desc,
                vmd.brnd_cd,
                vmd.brnd_desc,
                vmd.fran_cd,
                vmd.fran_desc,
                vmd.grp_fran_cd,
                vmd.grp_fran_desc,
                vmd.prod_fran_cd,
                vmd.prod_fran_desc,
                vmd.prod_mjr_cd,
                vmd.prod_mjr_desc,
                vmd.prod_mnr_cd,
                vmd.prod_mnr_desc,
                sales_derived.scan_units,
                sales_derived.scan_sales
            FROM 
                (
                    (
                        (
                            SELECT iss.iri_market,
                                iss.wk_end_dt,
                                iss.iri_prod_desc,
                                iss.iri_ean,
                                iss.ac_nielsencode,
                                iss.ac_code,
                                iss.ac_longname,
                                iss.sales_grp_cd,
                                iss.sales_grp_desc,
                                iss.scan_sales,
                                iss.scan_units,
                                etd.cal_date,
                                etd.time_id,
                                etd.jj_wk,
                                etd.jj_mnth,
                                etd.jj_mnth_shrt,
                                etd.jj_mnth_long,
                                etd.jj_qrtr,
                                etd.jj_year,
                                etd.cal_mnth_id,
                                etd.jj_mnth_id,
                                etd.cal_mnth,
                                etd.cal_qrtr,
                                etd.cal_year,
                                etd.jj_mnth_tot,
                                etd.jj_mnth_day,
                                etd.cal_mnth_nm,
                                etd.jj_mnth_wk,
                                etd.cal_wk,
                                etd.cal_mnth_wk,
                                ean.matl_id,
                                ean.bar_cd AS matl_bar_cd
                            FROM 
                                (
                                    SELECT etd.cal_date,
                                        etd.time_id,
                                        etd.jj_wk,
                                        etd.jj_mnth,
                                        etd.jj_mnth_shrt,
                                        etd.jj_mnth_long,
                                        etd.jj_qrtr,
                                        etd.jj_year,
                                        etd.cal_mnth_id,
                                        etd.jj_mnth_id,
                                        etd.cal_mnth,
                                        etd.cal_qrtr,
                                        etd.cal_year,
                                        etd.jj_mnth_tot,
                                        etd.jj_mnth_day,
                                        etd.cal_mnth_nm,
                                        etdw.jj_mnth_wk,
                                        etdc.cal_wk,
                                        etdcm.cal_mnth_wk
                                    FROM edw_time_dim etd,
                                    (
                                        SELECT etd.jj_year,
                                            etd.jj_mnth_id,
                                            etd.jj_wk,
                                            row_number() OVER(
                                                PARTITION BY etd.jj_year,
                                                etd.jj_mnth_id
                                                ORDER BY etd.jj_year,
                                                    etd.jj_mnth_id,
                                                    etd.jj_wk
                                            ) AS jj_mnth_wk
                                        FROM (
                                                SELECT DISTINCT etd.jj_year,
                                                    etd.jj_mnth_id,
                                                    etd.jj_wk
                                                FROM edw_time_dim etd
                                            ) etd
                                    ) etdw,
                                    (
                                        SELECT etd.cal_date,
                                            etd.time_id,
                                            etd.jj_wk,
                                            etd.jj_mnth,
                                            etd.jj_mnth_shrt,
                                            etd.jj_mnth_long,
                                            etd.jj_qrtr,
                                            etd.jj_year,
                                            etd.cal_mnth_id,
                                            etd.jj_mnth_id,
                                            etd.cal_mnth,
                                            etd.cal_qrtr,
                                            etd.cal_year,
                                            etd.jj_mnth_tot,
                                            etd.jj_mnth_day,
                                            etd.cal_mnth_nm,
                                            CASE
                                                WHEN (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY to_date(etd.cal_date)
                                                        ) % (7)::bigint
                                                    ) = 0
                                                ) THEN (
                                                    row_number() OVER(
                                                        PARTITION BY etd.cal_year
                                                        ORDER BY to_date(etd.cal_date)
                                                    ) / 7
                                                )
                                                ELSE (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY to_date(etd.cal_date)
                                                        ) / 7
                                                    ) + 1
                                                )
                                            END AS cal_wk
                                        FROM edw_time_dim etd
                                    ) etdc,
                                    (
                                        SELECT etdcw.cal_year,
                                            etdcw.cal_mnth_id,
                                            etdcw.cal_wk,
                                            row_number() OVER(
                                                PARTITION BY etdcw.cal_year,
                                                etdcw.cal_mnth_id
                                                ORDER BY etdcw.cal_year,
                                                    etdcw.cal_mnth_id,
                                                    etdcw.cal_wk
                                            ) AS cal_mnth_wk
                                        FROM (
                                                SELECT DISTINCT etdc.cal_year,
                                                    etdc.cal_mnth_id,
                                                    etdc.cal_wk
                                                FROM (
                                                        SELECT etd.cal_year,
                                                            etd.cal_mnth_id,
                                                            CASE
                                                                WHEN (
                                                                    (
                                                                        row_number() OVER(
                                                                            PARTITION BY etd.cal_year
                                                                            ORDER BY to_date(etd.cal_date)
                                                                        ) % (7)::bigint
                                                                    ) = 0
                                                                ) THEN (
                                                                    row_number() OVER(
                                                                        PARTITION BY etd.cal_year
                                                                        ORDER BY to_date(etd.cal_date)
                                                                    ) / 7
                                                                )
                                                                ELSE (
                                                                    (
                                                                        row_number() OVER(
                                                                            PARTITION BY etd.cal_year
                                                                            ORDER BY to_date(etd.cal_date)
                                                                        ) / 7
                                                                    ) + 1
                                                                )
                                                            END AS cal_wk
                                                        FROM edw_time_dim etd
                                                    ) etdc
                                            ) etdcw
                                    ) etdcm
                                    WHERE 
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (
                                                                (etd.jj_year = etdw.jj_year)
                                                                AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                            )
                                                            AND (etd.jj_wk = etdw.jj_wk)
                                                        )
                                                        AND (to_date(etd.cal_date) = to_date(etdc.cal_date))
                                                    )
                                                    AND (etdc.cal_wk = etdcm.cal_wk)
                                                )
                                                AND (etdc.cal_year = etdcm.cal_year)
                                            )
                                            AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                        )
                                ) etd,
                                (
                                    vw_iri_scan_sales iss
                                    LEFT JOIN 
                                    (
                                        SELECT edw_material_dim.matl_id,
                                            edw_material_dim.matl_desc,
                                            edw_material_dim.mega_brnd_cd,
                                            edw_material_dim.mega_brnd_desc,
                                            edw_material_dim.brnd_cd,
                                            edw_material_dim.brnd_desc,
                                            edw_material_dim.base_prod_cd,
                                            edw_material_dim.base_prod_desc,
                                            edw_material_dim.variant_cd,
                                            edw_material_dim.variant_desc,
                                            edw_material_dim.fran_cd,
                                            edw_material_dim.fran_desc,
                                            edw_material_dim.grp_fran_cd,
                                            edw_material_dim.grp_fran_desc,
                                            edw_material_dim.matl_type_cd,
                                            edw_material_dim.matl_type_desc,
                                            edw_material_dim.prod_fran_cd,
                                            edw_material_dim.prod_fran_desc,
                                            edw_material_dim.prod_hier_cd,
                                            edw_material_dim.prod_hier_desc,
                                            edw_material_dim.prod_mjr_cd,
                                            edw_material_dim.prod_mjr_desc,
                                            edw_material_dim.prod_mnr_cd,
                                            edw_material_dim.prod_mnr_desc,
                                            edw_material_dim.mercia_plan,
                                            edw_material_dim.putup_cd,
                                            edw_material_dim.putup_desc,
                                            edw_material_dim.bar_cd,
                                            edw_material_dim.updt_dt,
                                            edw_material_dim.prft_ctr
                                        FROM edw_material_dim
                                        WHERE 
                                            (
                                                edw_material_dim.bar_cd IN (
                                                    SELECT DISTINCT derived_table1.bar_cd
                                                    FROM (
                                                            SELECT count(*) AS count,
                                                                edw_material_dim.bar_cd
                                                            FROM edw_material_dim
                                                            GROUP BY edw_material_dim.bar_cd
                                                            HAVING (count(*) = 1)
                                                        ) derived_table1
                                                )
                                            )
                                    ) ean ON 
                                    (
                                        (
                                            ltrim(
                                                (ean.bar_cd)::text,
                                                ('0'::character varying)::text
                                            ) = ltrim(
                                                (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                ('0'::character varying)::text
                                            )
                                        )
                                    )
                                )
                            WHERE 
                                (
                                    to_date((iss.wk_end_dt)::timestamp without time zone) = to_date(etd.cal_date)
                                )
                        ) sales_derived
                        LEFT JOIN edw_material_dim vmd ON 
                        (
                            (
                                ltrim(
                                    (vmd.matl_id)::text,
                                    ('0'::character varying)::text
                                ) = ltrim(
                                    (
                                        COALESCE(sales_derived.matl_id, '0'::character varying)
                                    )::text,
                                    ('0'::character varying)::text
                                )
                            )
                        )
                    )
                    LEFT JOIN vw_customer_dim vcd ON (
                        (
                            ltrim(
                                (vcd.cust_no)::text,
                                ('0'::character varying)::text
                            ) = ltrim(
                                (sales_derived.ac_code)::text,
                                ('0'::character varying)::text
                            )
                        )
                    )
                )
            WHERE 
                (
                    sales_derived.matl_bar_cd IN 
                    (
                        SELECT DISTINCT derived_table2.bar_cd
                        FROM (
                                SELECT count(*) AS count,
                                    edw_material_dim.bar_cd
                                FROM edw_material_dim
                                GROUP BY edw_material_dim.bar_cd
                                HAVING (count(*) = 1)
                            ) derived_table2
                    )
                )
        ) bar_cd_map
    UNION ALL
    SELECT sales_derived.time_id,
        sales_derived.jj_year,
        sales_derived.jj_qrtr,
        sales_derived.jj_mnth,
        sales_derived.jj_wk,
        sales_derived.jj_mnth_wk,
        sales_derived.jj_mnth_id,
        sales_derived.jj_mnth_tot,
        sales_derived.jj_mnth_day,
        sales_derived.jj_mnth_shrt,
        sales_derived.jj_mnth_long,
        sales_derived.cal_year,
        sales_derived.cal_qrtr,
        sales_derived.cal_mnth,
        sales_derived.cal_wk,
        sales_derived.cal_mnth_wk,
        sales_derived.cal_mnth_id,
        sales_derived.cal_mnth_nm,
        sales_derived.wk_end_dt,
        sales_derived.ac_code AS ac_attribute,
        (vcd.cust_nm)::character varying AS cust_nm,
        vcd.channel_cd,
        vcd.channel_desc,
        sales_derived.sales_grp_cd,
        sales_derived.sales_grp_desc,
        vcd.cmp_id,
        vcd.country,
        sales_derived.iri_market,
        sales_derived.ac_nielsencode,
        sales_derived.ac_longname,
        sales_derived.iri_ean,
        NULL::character varying AS matl_bar_cd,
        sales_derived.iri_prod_desc,
        (
            ltrim(
                (vmd.matl_id)::text,
                ('0'::character varying)::text
            )
        )::character varying AS matl_id,
        vmd.matl_desc,
        vmd.brnd_cd,
        vmd.brnd_desc,
        vmd.fran_cd,
        vmd.fran_desc,
        vmd.grp_fran_cd,
        vmd.grp_fran_desc,
        vmd.prod_fran_cd,
        vmd.prod_fran_desc,
        vmd.prod_mjr_cd,
        vmd.prod_mjr_desc,
        vmd.prod_mnr_cd,
        vmd.prod_mnr_desc,
        sales_derived.scan_units,
        sales_derived.scan_sales
    FROM 
        (
            SELECT iss.iri_market,
                iss.wk_end_dt,
                iss.iri_prod_desc,
                iss.iri_ean,
                iss.ac_nielsencode,
                iss.ac_code,
                iss.ac_longname,
                iss.sales_grp_cd,
                iss.sales_grp_desc,
                iss.scan_sales,
                iss.scan_units,
                etd.cal_date,
                etd.time_id,
                etd.jj_wk,
                etd.jj_mnth,
                etd.jj_mnth_shrt,
                etd.jj_mnth_long,
                etd.jj_qrtr,
                etd.jj_year,
                etd.cal_mnth_id,
                etd.jj_mnth_id,
                etd.cal_mnth,
                etd.cal_qrtr,
                etd.cal_year,
                etd.jj_mnth_tot,
                etd.jj_mnth_day,
                etd.cal_mnth_nm,
                etd.jj_mnth_wk,
                etd.cal_wk,
                etd.cal_mnth_wk
            FROM 
                (
                    SELECT etd.cal_date,
                        etd.time_id,
                        etd.jj_wk,
                        etd.jj_mnth,
                        etd.jj_mnth_shrt,
                        etd.jj_mnth_long,
                        etd.jj_qrtr,
                        etd.jj_year,
                        etd.cal_mnth_id,
                        etd.jj_mnth_id,
                        etd.cal_mnth,
                        etd.cal_qrtr,
                        etd.cal_year,
                        etd.jj_mnth_tot,
                        etd.jj_mnth_day,
                        etd.cal_mnth_nm,
                        etdw.jj_mnth_wk,
                        etdc.cal_wk,
                        etdcm.cal_mnth_wk
                    FROM edw_time_dim etd,
                    (
                        SELECT etd.jj_year,
                            etd.jj_mnth_id,
                            etd.jj_wk,
                            row_number() OVER(
                                PARTITION BY etd.jj_year,
                                etd.jj_mnth_id
                                ORDER BY etd.jj_year,
                                    etd.jj_mnth_id,
                                    etd.jj_wk
                            ) AS jj_mnth_wk
                        FROM (
                                SELECT DISTINCT etd.jj_year,
                                    etd.jj_mnth_id,
                                    etd.jj_wk
                                FROM edw_time_dim etd
                            ) etd
                    ) etdw,
                    (
                        SELECT etd.cal_date,
                            etd.time_id,
                            etd.jj_wk,
                            etd.jj_mnth,
                            etd.jj_mnth_shrt,
                            etd.jj_mnth_long,
                            etd.jj_qrtr,
                            etd.jj_year,
                            etd.cal_mnth_id,
                            etd.jj_mnth_id,
                            etd.cal_mnth,
                            etd.cal_qrtr,
                            etd.cal_year,
                            etd.jj_mnth_tot,
                            etd.jj_mnth_day,
                            etd.cal_mnth_nm,
                            CASE
                                WHEN (
                                    (
                                        row_number() OVER(
                                            PARTITION BY etd.cal_year
                                            ORDER BY to_date(etd.cal_date)
                                        ) % (7)::bigint
                                    ) = 0
                                ) THEN (
                                    row_number() OVER(
                                        PARTITION BY etd.cal_year
                                        ORDER BY to_date(etd.cal_date)
                                    ) / 7
                                )
                                ELSE (
                                    (
                                        row_number() OVER(
                                            PARTITION BY etd.cal_year
                                            ORDER BY to_date(etd.cal_date)
                                        ) / 7
                                    ) + 1
                                )
                            END AS cal_wk
                        FROM edw_time_dim etd
                    ) etdc,
                    (
                        SELECT etdcw.cal_year,
                            etdcw.cal_mnth_id,
                            etdcw.cal_wk,
                            row_number() OVER(
                                PARTITION BY etdcw.cal_year,
                                etdcw.cal_mnth_id
                                ORDER BY etdcw.cal_year,
                                    etdcw.cal_mnth_id,
                                    etdcw.cal_wk
                            ) AS cal_mnth_wk
                        FROM 
                            (
                                SELECT DISTINCT etdc.cal_year,
                                    etdc.cal_mnth_id,
                                    etdc.cal_wk
                                FROM 
                                    (
                                        SELECT etd.cal_year,
                                            etd.cal_mnth_id,
                                            CASE
                                                WHEN (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY to_date(etd.cal_date)
                                                        ) % (7)::bigint
                                                    ) = 0
                                                ) THEN (
                                                    row_number() OVER(
                                                        PARTITION BY etd.cal_year
                                                        ORDER BY to_date(etd.cal_date)
                                                    ) / 7
                                                )
                                                ELSE (
                                                    (
                                                        row_number() OVER(
                                                            PARTITION BY etd.cal_year
                                                            ORDER BY to_date(etd.cal_date)
                                                        ) / 7
                                                    ) + 1
                                                )
                                            END AS cal_wk
                                        FROM edw_time_dim etd
                                    ) etdc
                            ) etdcw
                    ) etdcm
                    WHERE 
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (etd.jj_year = etdw.jj_year)
                                                AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                            )
                                            AND (etd.jj_wk = etdw.jj_wk)
                                        )
                                        AND (to_date(etd.cal_date) = to_date(etdc.cal_date))
                                    )
                                    AND (etdc.cal_wk = etdcm.cal_wk)
                                )
                                AND (etdc.cal_year = etdcm.cal_year)
                            )
                            AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                        )
                ) etd,
                vw_iri_scan_sales iss
            WHERE 
                (
                    to_date((iss.wk_end_dt)::timestamp without time zone) = to_date(etd.cal_date)
                )
        ) sales_derived
        LEFT JOIN 
        (
            (
                SELECT derived_table6.jj_month_id,
                    derived_table6.bar_cd,
                    derived_table6.cust_no,
                    derived_table6.material_id
                FROM 
                    (
                        SELECT DISTINCT derived_table5.jj_month_id,
                            derived_table5.bar_cd,
                            derived_table5.cust_no,
                            derived_table5.material_id
                        FROM (
                                SELECT DISTINCT derived_table4.jj_month_id,
                                    derived_table4.master_code,
                                    derived_table4.bar_cd,
                                    derived_table4.cust_no,
                                    derived_table4.material_count,
                                    derived_table4.gts_val,
                                    count(DISTINCT derived_table4.master_code) AS count,
                                    CASE
                                        WHEN (count(DISTINCT derived_table4.master_code) > 1) THEN CASE
                                            WHEN (
                                                (
                                                    (derived_table4.master_code IS NOT NULL)
                                                    AND (
                                                        derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                    )
                                                )
                                                AND (
                                                    upper((derived_table4.channel_desc)::text) <> ('AU - EXPORTS'::character varying)::text
                                                )
                                            ) THEN (derived_table4.max_material_id)::character varying
                                            WHEN (
                                                (
                                                    (derived_table4.master_code IS NULL)
                                                    AND (
                                                        derived_table4.gts_val < ((0)::numeric)::numeric(18, 0)
                                                    )
                                                )
                                                AND (
                                                    upper((derived_table4.channel_desc)::text) = ('AU - EXPORTS'::character varying)::text
                                                )
                                            ) THEN 'NULL'::character varying
                                            ELSE NULL::character varying
                                        END
                                        WHEN (count(DISTINCT derived_table4.master_code) = 1) THEN CASE
                                            WHEN (
                                                (derived_table4.material_count > 1)
                                                AND (
                                                    derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                )
                                            ) THEN (derived_table4.max_material_id)::character varying
                                            WHEN (derived_table4.material_count = 1) THEN (derived_table4.max_material_id)::character varying
                                            ELSE NULL::character varying
                                        END
                                        ELSE derived_table4.material_id
                                    END AS material_id
                                FROM (
                                        SELECT DISTINCT a.jj_month_id,
                                            a.master_code,
                                            a.bar_cd,
                                            a.matl_id AS material_id,
                                            a.cust_no,
                                            count(a.matl_id) OVER(
                                                PARTITION BY a.jj_month_id,
                                                a.bar_cd,
                                                a.cust_no 
                                                order by null
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                            ) AS material_count,
                                            "max"((a.matl_id)::text) OVER(
                                                PARTITION BY a.jj_month_id,
                                                a.master_code,
                                                a.bar_cd,
                                                a.cust_no,
                                                sum(a.gts_val) 
                                                order by null
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                            ) AS max_material_id,
                                            row_number() OVER(
                                                PARTITION BY a.jj_month_id,
                                                a.bar_cd,
                                                a.cust_no
                                                ORDER BY sum(a.gts_val) DESC
                                            ) AS sales_rank,
                                            count(COALESCE(a.master_code, 'NA'::character varying)) OVER(
                                                PARTITION BY a.jj_month_id,
                                                a.bar_cd,
                                                a.cust_no,
                                                a.matl_id 
                                                order by null
                                                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                            ) AS master_code_count,
                                            sum(a.gts_val) AS gts_val,
                                            a.channel_desc,
                                            b.matl_bar_count
                                        FROM 
                                            (
                                                (
                                                    SELECT vsf.jj_month_id,
                                                        vsf.gts_val,
                                                        vmd.matl_id,
                                                        vmd.bar_cd,
                                                        mstrcd.master_code,
                                                        vcd.cust_no,
                                                        vcd.channel_desc
                                                    FROM 
                                                    vw_sapbw_ciw_fact vsf
                                                    LEFT JOIN vw_customer_dim vcd ON (((vsf.cust_no)::text = (vcd.cust_no)::text))
                                                    LEFT JOIN edw_material_dim vmd ON (((vsf.matl_id)::text = (vmd.matl_id)::text))
                                                    LEFT JOIN 
                                                    (
                                                        vw_apo_parent_child_dim vapcd
                                                        LEFT JOIN 
                                                        (
                                                            SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                vw_apo_parent_child_dim.parent_matl_desc
                                                            FROM vw_apo_parent_child_dim
                                                            WHERE (
                                                                    (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                                )
                                                            UNION ALL
                                                            SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                vw_apo_parent_child_dim.parent_matl_desc
                                                            FROM vw_apo_parent_child_dim
                                                            WHERE 
                                                                (
                                                                    NOT (
                                                                        vw_apo_parent_child_dim.master_code IN (
                                                                            SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                                                            FROM vw_apo_parent_child_dim
                                                                            WHERE (
                                                                                    (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                                                )
                                                                        )
                                                                    )
                                                                )
                                                        ) mstrcd ON 
                                                        (
                                                            (
                                                                (vapcd.master_code)::text = (mstrcd.master_code)::text
                                                            )
                                                        )
                                                    ) ON 
                                                    (
                                                        (
                                                            ((vsf.cmp_id)::text = (vapcd.cmp_id)::text)
                                                            AND ((vsf.matl_id)::text = (vapcd.matl_id)::text)
                                                        )
                                                    )
                                                ) a
                                                JOIN 
                                                (
                                                    SELECT DISTINCT edw_material_dim.bar_cd,
                                                        count(DISTINCT edw_material_dim.matl_id) AS matl_bar_count
                                                    FROM edw_material_dim
                                                    WHERE (
                                                            COALESCE(edw_material_dim.bar_cd, 'NA'::character varying) IN (
                                                                SELECT DISTINCT COALESCE(derived_table3.bar_cd, 'NA'::character varying) AS "coalesce"
                                                                FROM (
                                                                        SELECT count(*) AS count,
                                                                            edw_material_dim.bar_cd
                                                                        FROM edw_material_dim
                                                                        GROUP BY edw_material_dim.bar_cd
                                                                        HAVING (count(*) > 1)
                                                                    ) derived_table3
                                                            )
                                                        )
                                                    GROUP BY edw_material_dim.bar_cd
                                                ) b ON (((a.bar_cd)::text = (b.bar_cd)::text))
                                            )
                                        GROUP BY a.jj_month_id,
                                            a.master_code,
                                            a.bar_cd,
                                            a.matl_id,
                                            a.cust_no,
                                            a.channel_desc,
                                            b.matl_bar_count
                                    ) derived_table4
                                WHERE (derived_table4.sales_rank = 1)
                                GROUP BY 
                                    derived_table4.jj_month_id,
                                    derived_table4.master_code,
                                    derived_table4.bar_cd,
                                    derived_table4.cust_no,
                                    derived_table4.material_id,
                                    derived_table4.material_count,
                                    derived_table4.max_material_id,
                                    derived_table4.master_code_count,
                                    derived_table4.channel_desc,
                                    derived_table4.gts_val,
                                    derived_table4.matl_bar_count
                            ) derived_table5
                    ) derived_table6
            ) ean
            LEFT JOIN edw_material_dim vmd ON (
                (
                    ltrim(
                        (vmd.matl_id)::text,
                        ('0'::character varying)::text
                    ) = ltrim(
                        (
                            COALESCE(ean.material_id, '0'::character varying)
                        )::text,
                        ('0'::character varying)::text
                    )
                )
            )
        ) ON 
        (
            (
                (
                    (
                        ((ean.jj_month_id)::numeric)::numeric(18, 0) = sales_derived.jj_mnth_id
                    )
                    AND (
                        ltrim(
                            (ean.cust_no)::text,
                            ('0'::character varying)::text
                        ) = (sales_derived.ac_code)::text
                    )
                )
                AND (
                    (COALESCE(ean.bar_cd, '0'::character varying))::text = (
                        COALESCE(sales_derived.iri_ean, '0'::character varying)
                    )::text
                )
            )
        )            
        LEFT JOIN vw_customer_dim vcd ON (
            (
                ltrim(
                    (vcd.cust_no)::text,
                    ('0'::character varying)::text
                ) = ltrim(
                    (sales_derived.ac_code)::text,
                    ('0'::character varying)::text
                )
            )
        )    
        WHERE 
            (
                NOT (
                    COALESCE(sales_derived.iri_ean, 'NA'::character varying) IN (
                        SELECT DISTINCT COALESCE(bar_cd_map.iri_ean, '0'::character varying) AS "coalesce"
                        FROM (
                                SELECT sales_derived.time_id,
                                    sales_derived.jj_year,
                                    sales_derived.jj_qrtr,
                                    sales_derived.jj_mnth,
                                    sales_derived.jj_wk,
                                    sales_derived.jj_mnth_wk,
                                    sales_derived.jj_mnth_id,
                                    sales_derived.jj_mnth_tot,
                                    sales_derived.jj_mnth_day,
                                    sales_derived.jj_mnth_shrt,
                                    sales_derived.jj_mnth_long,
                                    sales_derived.cal_year,
                                    sales_derived.cal_qrtr,
                                    sales_derived.cal_mnth,
                                    sales_derived.cal_wk,
                                    sales_derived.cal_mnth_wk,
                                    sales_derived.cal_mnth_id,
                                    sales_derived.cal_mnth_nm,
                                    sales_derived.wk_end_dt,
                                    sales_derived.ac_code AS ac_attribute,
                                    vcd.cust_nm,
                                    vcd.channel_cd,
                                    vcd.channel_desc,
                                    sales_derived.sales_grp_cd,
                                    sales_derived.sales_grp_desc,
                                    vcd.cmp_id,
                                    vcd.country,
                                    sales_derived.iri_market,
                                    sales_derived.ac_nielsencode,
                                    sales_derived.ac_longname,
                                    sales_derived.iri_ean,
                                    sales_derived.matl_bar_cd,
                                    sales_derived.iri_prod_desc,
                                    ltrim(
                                        (vmd.matl_id)::text,
                                        ('0'::character varying)::text
                                    ) AS matl_id,
                                    vmd.matl_desc,
                                    vmd.brnd_cd,
                                    vmd.brnd_desc,
                                    vmd.fran_cd,
                                    vmd.fran_desc,
                                    vmd.grp_fran_cd,
                                    vmd.grp_fran_desc,
                                    vmd.prod_fran_cd,
                                    vmd.prod_fran_desc,
                                    vmd.prod_mjr_cd,
                                    vmd.prod_mjr_desc,
                                    vmd.prod_mnr_cd,
                                    vmd.prod_mnr_desc,
                                    sales_derived.scan_units,
                                    sales_derived.scan_sales
                                FROM (
                                        (
                                            (
                                                SELECT iss.iri_market,
                                                    iss.wk_end_dt,
                                                    iss.iri_prod_desc,
                                                    iss.iri_ean,
                                                    iss.ac_nielsencode,
                                                    iss.ac_code,
                                                    iss.ac_longname,
                                                    iss.sales_grp_cd,
                                                    iss.sales_grp_desc,
                                                    iss.scan_sales,
                                                    iss.scan_units,
                                                    etd.cal_date,
                                                    etd.time_id,
                                                    etd.jj_wk,
                                                    etd.jj_mnth,
                                                    etd.jj_mnth_shrt,
                                                    etd.jj_mnth_long,
                                                    etd.jj_qrtr,
                                                    etd.jj_year,
                                                    etd.cal_mnth_id,
                                                    etd.jj_mnth_id,
                                                    etd.cal_mnth,
                                                    etd.cal_qrtr,
                                                    etd.cal_year,
                                                    etd.jj_mnth_tot,
                                                    etd.jj_mnth_day,
                                                    etd.cal_mnth_nm,
                                                    etd.jj_mnth_wk,
                                                    etd.cal_wk,
                                                    etd.cal_mnth_wk,
                                                    ean.matl_id,
                                                    ean.bar_cd AS matl_bar_cd
                                                FROM (
                                                        SELECT etd.cal_date,
                                                            etd.time_id,
                                                            etd.jj_wk,
                                                            etd.jj_mnth,
                                                            etd.jj_mnth_shrt,
                                                            etd.jj_mnth_long,
                                                            etd.jj_qrtr,
                                                            etd.jj_year,
                                                            etd.cal_mnth_id,
                                                            etd.jj_mnth_id,
                                                            etd.cal_mnth,
                                                            etd.cal_qrtr,
                                                            etd.cal_year,
                                                            etd.jj_mnth_tot,
                                                            etd.jj_mnth_day,
                                                            etd.cal_mnth_nm,
                                                            etdw.jj_mnth_wk,
                                                            etdc.cal_wk,
                                                            etdcm.cal_mnth_wk
                                                        FROM edw_time_dim etd,
                                                            (
                                                                SELECT etd.jj_year,
                                                                    etd.jj_mnth_id,
                                                                    etd.jj_wk,
                                                                    row_number() OVER(
                                                                        PARTITION BY etd.jj_year,
                                                                        etd.jj_mnth_id
                                                                        ORDER BY etd.jj_year,
                                                                            etd.jj_mnth_id,
                                                                            etd.jj_wk
                                                                    ) AS jj_mnth_wk
                                                                FROM (
                                                                        SELECT DISTINCT etd.jj_year,
                                                                            etd.jj_mnth_id,
                                                                            etd.jj_wk
                                                                        FROM edw_time_dim etd
                                                                    ) etd
                                                            ) etdw,
                                                            (
                                                                SELECT etd.cal_date,
                                                                    etd.time_id,
                                                                    etd.jj_wk,
                                                                    etd.jj_mnth,
                                                                    etd.jj_mnth_shrt,
                                                                    etd.jj_mnth_long,
                                                                    etd.jj_qrtr,
                                                                    etd.jj_year,
                                                                    etd.cal_mnth_id,
                                                                    etd.jj_mnth_id,
                                                                    etd.cal_mnth,
                                                                    etd.cal_qrtr,
                                                                    etd.cal_year,
                                                                    etd.jj_mnth_tot,
                                                                    etd.jj_mnth_day,
                                                                    etd.cal_mnth_nm,
                                                                    CASE
                                                                        WHEN (
                                                                            (
                                                                                row_number() OVER(
                                                                                    PARTITION BY etd.cal_year
                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                ) % (7)::bigint
                                                                            ) = 0
                                                                        ) THEN (
                                                                            row_number() OVER(
                                                                                PARTITION BY etd.cal_year
                                                                                ORDER BY to_date(etd.cal_date)
                                                                            ) / 7
                                                                        )
                                                                        ELSE (
                                                                            (
                                                                                row_number() OVER(
                                                                                    PARTITION BY etd.cal_year
                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                ) / 7
                                                                            ) + 1
                                                                        )
                                                                    END AS cal_wk
                                                                FROM edw_time_dim etd
                                                            ) etdc,
                                                            (
                                                                SELECT etdcw.cal_year,
                                                                    etdcw.cal_mnth_id,
                                                                    etdcw.cal_wk,
                                                                    row_number() OVER(
                                                                        PARTITION BY etdcw.cal_year,
                                                                        etdcw.cal_mnth_id
                                                                        ORDER BY etdcw.cal_year,
                                                                            etdcw.cal_mnth_id,
                                                                            etdcw.cal_wk
                                                                    ) AS cal_mnth_wk
                                                                FROM (
                                                                        SELECT DISTINCT etdc.cal_year,
                                                                            etdc.cal_mnth_id,
                                                                            etdc.cal_wk
                                                                        FROM (
                                                                                SELECT etd.cal_year,
                                                                                    etd.cal_mnth_id,
                                                                                    CASE
                                                                                        WHEN (
                                                                                            (
                                                                                                row_number() OVER(
                                                                                                    PARTITION BY etd.cal_year
                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                ) % (7)::bigint
                                                                                            ) = 0
                                                                                        ) THEN (
                                                                                            row_number() OVER(
                                                                                                PARTITION BY etd.cal_year
                                                                                                ORDER BY to_date(etd.cal_date)
                                                                                            ) / 7
                                                                                        )
                                                                                        ELSE (
                                                                                            (
                                                                                                row_number() OVER(
                                                                                                    PARTITION BY etd.cal_year
                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                ) / 7
                                                                                            ) + 1
                                                                                        )
                                                                                    END AS cal_wk
                                                                                FROM edw_time_dim etd
                                                                            ) etdc
                                                                    ) etdcw
                                                            ) etdcm
                                                        WHERE (
                                                                (
                                                                    (
                                                                        (
                                                                            (
                                                                                (
                                                                                    (etd.jj_year = etdw.jj_year)
                                                                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                )
                                                                                AND (etd.jj_wk = etdw.jj_wk)
                                                                            )
                                                                            AND (to_date(etd.cal_date) = to_date(etdc.cal_date))
                                                                        )
                                                                        AND (etdc.cal_wk = etdcm.cal_wk)
                                                                    )
                                                                    AND (etdc.cal_year = etdcm.cal_year)
                                                                )
                                                                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                            )
                                                    ) etd,
                                                    (
                                                        vw_iri_scan_sales iss
                                                        LEFT JOIN (
                                                            SELECT edw_material_dim.matl_id,
                                                                edw_material_dim.matl_desc,
                                                                edw_material_dim.mega_brnd_cd,
                                                                edw_material_dim.mega_brnd_desc,
                                                                edw_material_dim.brnd_cd,
                                                                edw_material_dim.brnd_desc,
                                                                edw_material_dim.base_prod_cd,
                                                                edw_material_dim.base_prod_desc,
                                                                edw_material_dim.variant_cd,
                                                                edw_material_dim.variant_desc,
                                                                edw_material_dim.fran_cd,
                                                                edw_material_dim.fran_desc,
                                                                edw_material_dim.grp_fran_cd,
                                                                edw_material_dim.grp_fran_desc,
                                                                edw_material_dim.matl_type_cd,
                                                                edw_material_dim.matl_type_desc,
                                                                edw_material_dim.prod_fran_cd,
                                                                edw_material_dim.prod_fran_desc,
                                                                edw_material_dim.prod_hier_cd,
                                                                edw_material_dim.prod_hier_desc,
                                                                edw_material_dim.prod_mjr_cd,
                                                                edw_material_dim.prod_mjr_desc,
                                                                edw_material_dim.prod_mnr_cd,
                                                                edw_material_dim.prod_mnr_desc,
                                                                edw_material_dim.mercia_plan,
                                                                edw_material_dim.putup_cd,
                                                                edw_material_dim.putup_desc,
                                                                edw_material_dim.bar_cd,
                                                                edw_material_dim.updt_dt,
                                                                edw_material_dim.prft_ctr
                                                            FROM edw_material_dim
                                                            WHERE (
                                                                    edw_material_dim.bar_cd IN (
                                                                        SELECT DISTINCT derived_table1.bar_cd
                                                                        FROM (
                                                                                SELECT count(*) AS count,
                                                                                    edw_material_dim.bar_cd
                                                                                FROM edw_material_dim
                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                HAVING (count(*) = 1)
                                                                            ) derived_table1
                                                                    )
                                                                )
                                                        ) ean ON (
                                                            (
                                                                ltrim(
                                                                    (ean.bar_cd)::text,
                                                                    ('0'::character varying)::text
                                                                ) = ltrim(
                                                                    (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                    ('0'::character varying)::text
                                                                )
                                                            )
                                                        )
                                                    )
                                                WHERE (
                                                        to_date((iss.wk_end_dt)::timestamp without time zone) = to_date(etd.cal_date)
                                                    )
                                            ) sales_derived
                                            LEFT JOIN edw_material_dim vmd ON (
                                                (
                                                    ltrim(
                                                        (vmd.matl_id)::text,
                                                        ('0'::character varying)::text
                                                    ) = ltrim(
                                                        (
                                                            COALESCE(sales_derived.matl_id, '0'::character varying)
                                                        )::text,
                                                        ('0'::character varying)::text
                                                    )
                                                )
                                            )
                                        )
                                        LEFT JOIN vw_customer_dim vcd ON (
                                            (
                                                ltrim(
                                                    (vcd.cust_no)::text,
                                                    ('0'::character varying)::text
                                                ) = ltrim(
                                                    (sales_derived.ac_code)::text,
                                                    ('0'::character varying)::text
                                                )
                                            )
                                        )
                                    )
                                WHERE (
                                        sales_derived.matl_bar_cd IN (
                                            SELECT DISTINCT derived_table2.bar_cd
                                            FROM (
                                                    SELECT count(*) AS count,
                                                        edw_material_dim.bar_cd
                                                    FROM edw_material_dim
                                                    GROUP BY edw_material_dim.bar_cd
                                                    HAVING (count(*) = 1)
                                                ) derived_table2
                                        )
                                    )
                            ) bar_cd_map
                    )
                )
            )
),
issa as 
(
    SELECT 
        sales_cte.time_id,
        sales_cte.jj_year,
        sales_cte.jj_qrtr,
        sales_cte.jj_mnth,
        sales_cte.jj_wk,
        sales_cte.jj_mnth_wk,
        sales_cte.jj_mnth_id,
        sales_cte.jj_mnth_tot,
        sales_cte.jj_mnth_day,
        sales_cte.jj_mnth_shrt,
        sales_cte.jj_mnth_long,
        sales_cte.cal_year,
        sales_cte.cal_qrtr,
        sales_cte.cal_mnth,
        sales_cte.cal_wk,
        sales_cte.cal_mnth_wk,
        sales_cte.cal_mnth_id,
        sales_cte.cal_mnth_nm,
        sales_cte.wk_end_dt,
        sales_cte.representative_cust_cd,
        sales_cte.representative_cust_nm,
        sales_cte.channel_cd,
        sales_cte.channel_desc,
        sales_cte.country,
        sales_cte.sales_grp_cd,
        sales_cte.sales_grp_nm,
        sales_cte.cmp_id,
        sales_cte.iri_market,
        sales_cte.ac_nielsencode,
        sales_cte.ac_longname,
        sales_cte.iri_ean,
        sales_cte.iri_prod_desc,
        sales_cte.matl_id,
        sales_cte.matl_desc,
        sales_cte.master_code,
        sales_cte.parent_matl_id,
        sales_cte.parent_matl_desc,
        sales_cte.brnd_cd,
        sales_cte.brnd_desc,
        sales_cte.fran_cd,
        sales_cte.fran_desc,
        sales_cte.grp_fran_cd,
        sales_cte.grp_fran_desc,
        sales_cte.prod_fran_cd,
        sales_cte.prod_fran_desc,
        sales_cte.prod_mjr_cd,
        sales_cte.prod_mjr_desc,
        sales_cte.prod_mnr_cd,
        sales_cte.prod_mnr_desc,
        sales_cte.scan_units,
        sales_cte.scan_sales,
        COALESCE(
            CASE
                WHEN (
                    (mat_dim.pka_product_key)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE mat_dim.pka_product_key
            END,
            CASE
                WHEN (
                    (prod_key.pka_productkey)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE prod_key.pka_productkey
            END
        ) AS pka_productkey,
        COALESCE(
            CASE
                WHEN (
                    (mat_dim.pka_product_key_description)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE mat_dim.pka_product_key_description
            END,
            CASE
                WHEN (
                    (prod_key.pka_productdesc)::text = (''::character varying)::text
                ) THEN NULL::character varying
                ELSE prod_key.pka_productdesc
            END
        ) AS pka_productdesc,
        COALESCE(
            CASE
                WHEN (prod_key.sku = (''::character varying)::text) THEN (NULL::character varying)::text
                ELSE ltrim(prod_key.sku, ('0'::character varying)::text)
            END,
            (
                (
                    CASE
                        WHEN (
                            (mat_dim.matl_num)::text = (''::character varying)::text
                        ) THEN (NULL::character varying)::text
                        ELSE ltrim(
                            (mat_dim.matl_num)::text,
                            ('0'::character varying)::text
                        )
                    END
                )::character varying
            )::text
        ) AS lst_sku,
        mat_dim.gph_region,
        mat_dim.gph_reg_frnchse,
        mat_dim.gph_reg_frnchse_grp,
        mat_dim.gph_prod_frnchse,
        mat_dim.gph_prod_brnd,
        mat_dim.gph_prod_sub_brnd,
        mat_dim.gph_prod_vrnt,
        mat_dim.gph_prod_needstate,
        mat_dim.gph_prod_ctgry,
        mat_dim.gph_prod_subctgry,
        mat_dim.gph_prod_sgmnt,
        mat_dim.gph_prod_subsgmnt,
        mat_dim.gph_prod_put_up_cd,
        mat_dim.gph_prod_put_up_desc
    FROM 
        (
            (
                (
                    SELECT sales.time_id,
                        sales.jj_year,
                        sales.jj_qrtr,
                        sales.jj_mnth,
                        sales.jj_wk,
                        sales.jj_mnth_wk,
                        sales.jj_mnth_id,
                        sales.jj_mnth_tot,
                        sales.jj_mnth_day,
                        sales.jj_mnth_shrt,
                        sales.jj_mnth_long,
                        sales.cal_year,
                        sales.cal_qrtr,
                        sales.cal_mnth,
                        sales.cal_wk,
                        sales.cal_mnth_wk,
                        sales.cal_mnth_id,
                        sales.cal_mnth_nm,
                        sales.wk_end_dt,
                        sales.ac_attribute AS representative_cust_cd,
                        sales.cust_nm AS representative_cust_nm,
                        sales.channel_cd,
                        sales.channel_desc,
                        sales.country,
                        sales.sales_grp_cd,
                        sales.sales_grp_desc AS sales_grp_nm,
                        sales.cmp_id,
                        sales.iri_market,
                        sales.ac_nielsencode,
                        sales.ac_longname,
                        sales.iri_ean,
                        sales.iri_prod_desc,
                        ltrim(
                            (sales.matl_id)::text,
                            ('0'::character varying)::text
                        ) AS matl_id,
                        sales.matl_desc,
                        matl.master_code,
                        ltrim(
                            (matl.parent_id)::text,
                            ('0'::character varying)::text
                        ) AS parent_matl_id,
                        matl.parent_matl_desc,
                        sales.brnd_cd,
                        sales.brnd_desc,
                        sales.fran_cd,
                        sales.fran_desc,
                        sales.grp_fran_cd,
                        sales.grp_fran_desc,
                        sales.prod_fran_cd,
                        sales.prod_fran_desc,
                        sales.prod_mjr_cd,
                        sales.prod_mjr_desc,
                        sales.prod_mnr_cd,
                        sales.prod_mnr_desc,
                        sales.scan_units,
                        sales.scan_sales
                    FROM sales
                        LEFT JOIN
                        (
                            SELECT vapcd.sales_org,
                                vapcd.cmp_id,
                                vapcd.matl_id,
                                vapcd.matl_desc,
                                vapcd.master_code,
                                vapcd.launch_date,
                                vapcd.predessor_id,
                                vapcd.parent_id,
                                vapcd.parent_matl_desc
                            FROM 
                                (
                                    vw_apo_parent_child_dim vapcd
                                    LEFT JOIN 
                                    (
                                        SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                            vw_apo_parent_child_dim.parent_matl_desc
                                        FROM vw_apo_parent_child_dim
                                        WHERE (
                                                (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                            )
                                        UNION ALL
                                        SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                            vw_apo_parent_child_dim.parent_matl_desc
                                        FROM vw_apo_parent_child_dim
                                        WHERE (
                                                NOT (
                                                    vw_apo_parent_child_dim.master_code IN (
                                                        SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                                        FROM vw_apo_parent_child_dim
                                                        WHERE (
                                                                (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                            )
                                                    )
                                                )
                                            )
                                    ) mstr ON (
                                        (
                                            (vapcd.master_code)::text = (mstr.master_code)::text
                                        )
                                    )
                                )
                        ) matl ON 
                        (
                            (
                                ((matl.cmp_id)::text = (sales.cmp_id)::text)
                                AND (
                                    ltrim(
                                        (matl.matl_id)::text,
                                        ('0'::character varying)::text
                                    ) = ltrim(
                                        (sales.matl_id)::text,
                                        ('0'::character varying)::text
                                    )
                                )
                            )
                        )
                ) sales_cte
                LEFT JOIN 
                (
                    SELECT DISTINCT a.ean_upc,
                        a.sku,
                        a.pka_productkey,
                        a.pka_productdesc
                    FROM 
                        (
                            (
                                SELECT ltrim(
                                        (edw_product_key_attributes.ean_upc)::text,
                                        ('0'::character varying)::text
                                    ) AS ean_upc,
                                    ltrim(
                                        (edw_product_key_attributes.matl_num)::text,
                                        ('0'::character varying)::text
                                    ) AS sku,
                                    edw_product_key_attributes.pka_productkey,
                                    edw_product_key_attributes.pka_productdesc,
                                    edw_product_key_attributes.lst_nts AS nts_date
                                FROM edw_product_key_attributes
                                WHERE 
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (edw_product_key_attributes.matl_type_cd)::text = ('FERT'::character varying)::text
                                                    )
                                                    OR (
                                                        (edw_product_key_attributes.matl_type_cd)::text = ('HALB'::character varying)::text
                                                    )
                                                )
                                                OR (
                                                    (edw_product_key_attributes.matl_type_cd)::text = ('SAPR'::character varying)::text
                                                )
                                            )
                                            AND (edw_product_key_attributes.lst_nts IS NOT NULL)
                                        )
                                        AND (
                                            (
                                                (edw_product_key_attributes.ctry_nm)::text = ('Australia'::character varying)::text
                                            )
                                            OR (
                                                (edw_product_key_attributes.ctry_nm)::text = ('New Zealand'::character varying)::text
                                            )
                                        )
                                    )
                            ) a
                            JOIN 
                            (
                                SELECT ltrim(
                                        (edw_product_key_attributes.ean_upc)::text,
                                        ('0'::character varying)::text
                                    ) AS ean_upc,
                                    ltrim(
                                        (edw_product_key_attributes.matl_num)::text,
                                        ('0'::character varying)::text
                                    ) AS sku,
                                    edw_product_key_attributes.lst_nts AS latest_nts_date,
                                    row_number() OVER(
                                        PARTITION BY edw_product_key_attributes.ean_upc
                                        ORDER BY edw_product_key_attributes.lst_nts DESC
                                    ) AS row_number
                                FROM edw_product_key_attributes
                                WHERE 
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (edw_product_key_attributes.matl_type_cd)::text = ('FERT'::character varying)::text
                                                    )
                                                    OR (
                                                        (edw_product_key_attributes.matl_type_cd)::text = ('HALB'::character varying)::text
                                                    )
                                                )
                                                OR (
                                                    (edw_product_key_attributes.matl_type_cd)::text = ('SAPR'::character varying)::text
                                                )
                                            )
                                            AND (edw_product_key_attributes.lst_nts IS NOT NULL)
                                        )
                                        AND (
                                            (
                                                (edw_product_key_attributes.ctry_nm)::text = ('Australia'::character varying)::text
                                            )
                                            OR (
                                                (edw_product_key_attributes.ctry_nm)::text = ('New Zealand'::character varying)::text
                                            )
                                        )
                                    )
                            ) b ON 
                            (
                                (
                                    (
                                        (
                                            (a.ean_upc = b.ean_upc)
                                            AND (a.sku = b.sku)
                                        )
                                        AND (b.latest_nts_date = a.nts_date)
                                    )
                                    AND (b.row_number = 1)
                                )
                            )
                        )
                ) prod_key ON 
                (
                    (
                        ltrim(
                            (sales_cte.iri_ean)::text,
                            ('0'::character varying)::text
                        ) = ltrim(prod_key.ean_upc, ('0'::character varying)::text)
                    )
                )
            )
            LEFT JOIN 
            (
                SELECT derived_table2.iri_ean,
                    derived_table2.matl_id,
                    derived_table2.matl_num,
                    derived_table2.crt_on,
                    derived_table2.pka_product_key,
                    derived_table2.pka_product_key_description,
                    derived_table2.gph_region,
                    derived_table2.gph_reg_frnchse,
                    derived_table2.gph_reg_frnchse_grp,
                    derived_table2.gph_prod_frnchse,
                    derived_table2.gph_prod_brnd,
                    derived_table2.gph_prod_sub_brnd,
                    derived_table2.gph_prod_vrnt,
                    derived_table2.gph_prod_needstate,
                    derived_table2.gph_prod_ctgry,
                    derived_table2.gph_prod_subctgry,
                    derived_table2.gph_prod_sgmnt,
                    derived_table2.gph_prod_subsgmnt,
                    derived_table2.gph_prod_put_up_cd,
                    derived_table2.gph_prod_put_up_desc,
                    derived_table2.rnk
                FROM 
                    (
                        SELECT a.iri_ean,
                            a.matl_id,
                            b.matl_num,
                            b.crt_on,
                            b.pka_product_key,
                            b.pka_product_key_description,
                            b.gph_region,
                            b.gph_reg_frnchse,
                            b.gph_reg_frnchse_grp,
                            b.gph_prod_frnchse,
                            b.gph_prod_brnd,
                            b.gph_prod_sub_brnd,
                            b.gph_prod_vrnt,
                            b.gph_prod_needstate,
                            b.gph_prod_ctgry,
                            b.gph_prod_subctgry,
                            b.gph_prod_sgmnt,
                            b.gph_prod_subsgmnt,
                            b.gph_prod_put_up_cd,
                            b.gph_prod_put_up_desc,
                            row_number() OVER(
                                PARTITION BY a.iri_ean
                                ORDER BY b.crt_on DESC NULLS LAST
                            ) AS rnk
                        FROM (
                                (
                                    SELECT DISTINCT iri_scan_sales_analysis_cte.iri_ean,
                                        iri_scan_sales_analysis_cte.matl_id
                                    FROM (
                                            SELECT sales.time_id,
                                                sales.jj_year,
                                                sales.jj_qrtr,
                                                sales.jj_mnth,
                                                sales.jj_wk,
                                                sales.jj_mnth_wk,
                                                sales.jj_mnth_id,
                                                sales.jj_mnth_tot,
                                                sales.jj_mnth_day,
                                                sales.jj_mnth_shrt,
                                                sales.jj_mnth_long,
                                                sales.cal_year,
                                                sales.cal_qrtr,
                                                sales.cal_mnth,
                                                sales.cal_wk,
                                                sales.cal_mnth_wk,
                                                sales.cal_mnth_id,
                                                sales.cal_mnth_nm,
                                                sales.wk_end_dt,
                                                sales.ac_attribute AS representative_cust_cd,
                                                sales.cust_nm AS representative_cust_nm,
                                                sales.channel_cd,
                                                sales.channel_desc,
                                                sales.country,
                                                sales.sales_grp_cd,
                                                sales.sales_grp_desc AS sales_grp_nm,
                                                sales.iri_market,
                                                sales.ac_nielsencode,
                                                sales.ac_longname,
                                                sales.iri_ean,
                                                sales.iri_prod_desc,
                                                ltrim(
                                                    (sales.matl_id)::text,
                                                    ('0'::character varying)::text
                                                ) AS matl_id,
                                                sales.matl_desc,
                                                matl.master_code,
                                                ltrim(
                                                    (matl.parent_id)::text,
                                                    ('0'::character varying)::text
                                                ) AS parent_matl_id,
                                                matl.parent_matl_desc,
                                                sales.brnd_cd,
                                                sales.brnd_desc,
                                                sales.fran_cd,
                                                sales.fran_desc,
                                                sales.grp_fran_cd,
                                                sales.grp_fran_desc,
                                                sales.prod_fran_cd,
                                                sales.prod_fran_desc,
                                                sales.prod_mjr_cd,
                                                sales.prod_mjr_desc,
                                                sales.prod_mnr_cd,
                                                sales.prod_mnr_desc,
                                                sales.scan_units,
                                                sales.scan_sales
                                            FROM (
                                                    (
                                                        SELECT bar_cd_map.time_id,
                                                            bar_cd_map.jj_year,
                                                            bar_cd_map.jj_qrtr,
                                                            bar_cd_map.jj_mnth,
                                                            bar_cd_map.jj_wk,
                                                            bar_cd_map.jj_mnth_wk,
                                                            bar_cd_map.jj_mnth_id,
                                                            bar_cd_map.jj_mnth_tot,
                                                            bar_cd_map.jj_mnth_day,
                                                            bar_cd_map.jj_mnth_shrt,
                                                            bar_cd_map.jj_mnth_long,
                                                            bar_cd_map.cal_year,
                                                            bar_cd_map.cal_qrtr,
                                                            bar_cd_map.cal_mnth,
                                                            bar_cd_map.cal_wk,
                                                            bar_cd_map.cal_mnth_wk,
                                                            bar_cd_map.cal_mnth_id,
                                                            bar_cd_map.cal_mnth_nm,
                                                            bar_cd_map.wk_end_dt,
                                                            bar_cd_map.ac_attribute,
                                                            (bar_cd_map.cust_nm)::character varying AS cust_nm,
                                                            bar_cd_map.channel_cd,
                                                            bar_cd_map.channel_desc,
                                                            bar_cd_map.sales_grp_cd,
                                                            bar_cd_map.sales_grp_desc,
                                                            bar_cd_map.cmp_id,
                                                            bar_cd_map.country,
                                                            bar_cd_map.iri_market,
                                                            bar_cd_map.ac_nielsencode,
                                                            bar_cd_map.ac_longname,
                                                            bar_cd_map.iri_ean,
                                                            bar_cd_map.matl_bar_cd,
                                                            bar_cd_map.iri_prod_desc,
                                                            (bar_cd_map.matl_id)::character varying AS matl_id,
                                                            bar_cd_map.matl_desc,
                                                            bar_cd_map.brnd_cd,
                                                            bar_cd_map.brnd_desc,
                                                            bar_cd_map.fran_cd,
                                                            bar_cd_map.fran_desc,
                                                            bar_cd_map.grp_fran_cd,
                                                            bar_cd_map.grp_fran_desc,
                                                            bar_cd_map.prod_fran_cd,
                                                            bar_cd_map.prod_fran_desc,
                                                            bar_cd_map.prod_mjr_cd,
                                                            bar_cd_map.prod_mjr_desc,
                                                            bar_cd_map.prod_mnr_cd,
                                                            bar_cd_map.prod_mnr_desc,
                                                            bar_cd_map.scan_units,
                                                            bar_cd_map.scan_sales
                                                        FROM (
                                                                SELECT sales_derived.time_id,
                                                                    sales_derived.jj_year,
                                                                    sales_derived.jj_qrtr,
                                                                    sales_derived.jj_mnth,
                                                                    sales_derived.jj_wk,
                                                                    sales_derived.jj_mnth_wk,
                                                                    sales_derived.jj_mnth_id,
                                                                    sales_derived.jj_mnth_tot,
                                                                    sales_derived.jj_mnth_day,
                                                                    sales_derived.jj_mnth_shrt,
                                                                    sales_derived.jj_mnth_long,
                                                                    sales_derived.cal_year,
                                                                    sales_derived.cal_qrtr,
                                                                    sales_derived.cal_mnth,
                                                                    sales_derived.cal_wk,
                                                                    sales_derived.cal_mnth_wk,
                                                                    sales_derived.cal_mnth_id,
                                                                    sales_derived.cal_mnth_nm,
                                                                    sales_derived.wk_end_dt,
                                                                    sales_derived.ac_code AS ac_attribute,
                                                                    vcd.cust_nm,
                                                                    vcd.channel_cd,
                                                                    vcd.channel_desc,
                                                                    sales_derived.sales_grp_cd,
                                                                    sales_derived.sales_grp_desc,
                                                                    vcd.cmp_id,
                                                                    vcd.country,
                                                                    sales_derived.iri_market,
                                                                    sales_derived.ac_nielsencode,
                                                                    sales_derived.ac_longname,
                                                                    sales_derived.iri_ean,
                                                                    sales_derived.matl_bar_cd,
                                                                    sales_derived.iri_prod_desc,
                                                                    ltrim(
                                                                        (vmd.matl_id)::text,
                                                                        ('0'::character varying)::text
                                                                    ) AS matl_id,
                                                                    vmd.matl_desc,
                                                                    vmd.brnd_cd,
                                                                    vmd.brnd_desc,
                                                                    vmd.fran_cd,
                                                                    vmd.fran_desc,
                                                                    vmd.grp_fran_cd,
                                                                    vmd.grp_fran_desc,
                                                                    vmd.prod_fran_cd,
                                                                    vmd.prod_fran_desc,
                                                                    vmd.prod_mjr_cd,
                                                                    vmd.prod_mjr_desc,
                                                                    vmd.prod_mnr_cd,
                                                                    vmd.prod_mnr_desc,
                                                                    sales_derived.scan_units,
                                                                    sales_derived.scan_sales
                                                                FROM (
                                                                        (
                                                                            (
                                                                                SELECT iss.iri_market,
                                                                                    iss.wk_end_dt,
                                                                                    iss.iri_prod_desc,
                                                                                    iss.iri_ean,
                                                                                    iss.ac_nielsencode,
                                                                                    iss.ac_code,
                                                                                    iss.ac_longname,
                                                                                    iss.sales_grp_cd,
                                                                                    iss.sales_grp_desc,
                                                                                    iss.scan_sales,
                                                                                    iss.scan_units,
                                                                                    etd.cal_date,
                                                                                    etd.time_id,
                                                                                    etd.jj_wk,
                                                                                    etd.jj_mnth,
                                                                                    etd.jj_mnth_shrt,
                                                                                    etd.jj_mnth_long,
                                                                                    etd.jj_qrtr,
                                                                                    etd.jj_year,
                                                                                    etd.cal_mnth_id,
                                                                                    etd.jj_mnth_id,
                                                                                    etd.cal_mnth,
                                                                                    etd.cal_qrtr,
                                                                                    etd.cal_year,
                                                                                    etd.jj_mnth_tot,
                                                                                    etd.jj_mnth_day,
                                                                                    etd.cal_mnth_nm,
                                                                                    etd.jj_mnth_wk,
                                                                                    etd.cal_wk,
                                                                                    etd.cal_mnth_wk,
                                                                                    ean.matl_id,
                                                                                    ean.bar_cd AS matl_bar_cd
                                                                                FROM (
                                                                                        SELECT etd.cal_date,
                                                                                            etd.time_id,
                                                                                            etd.jj_wk,
                                                                                            etd.jj_mnth,
                                                                                            etd.jj_mnth_shrt,
                                                                                            etd.jj_mnth_long,
                                                                                            etd.jj_qrtr,
                                                                                            etd.jj_year,
                                                                                            etd.cal_mnth_id,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.cal_mnth,
                                                                                            etd.cal_qrtr,
                                                                                            etd.cal_year,
                                                                                            etd.jj_mnth_tot,
                                                                                            etd.jj_mnth_day,
                                                                                            etd.cal_mnth_nm,
                                                                                            etdw.jj_mnth_wk,
                                                                                            etdc.cal_wk,
                                                                                            etdcm.cal_mnth_wk
                                                                                        FROM edw_time_dim etd,
                                                                                            (
                                                                                                SELECT etd.jj_year,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.jj_wk,
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY etd.jj_year,
                                                                                                        etd.jj_mnth_id
                                                                                                        ORDER BY etd.jj_year,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.jj_wk
                                                                                                    ) AS jj_mnth_wk
                                                                                                FROM (
                                                                                                        SELECT DISTINCT etd.jj_year,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.jj_wk
                                                                                                        FROM edw_time_dim etd
                                                                                                    ) etd
                                                                                            ) etdw,
                                                                                            (
                                                                                                SELECT etd.cal_date,
                                                                                                    etd.time_id,
                                                                                                    etd.jj_wk,
                                                                                                    etd.jj_mnth,
                                                                                                    etd.jj_mnth_shrt,
                                                                                                    etd.jj_mnth_long,
                                                                                                    etd.jj_qrtr,
                                                                                                    etd.jj_year,
                                                                                                    etd.cal_mnth_id,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.cal_mnth,
                                                                                                    etd.cal_qrtr,
                                                                                                    etd.cal_year,
                                                                                                    etd.jj_mnth_tot,
                                                                                                    etd.jj_mnth_day,
                                                                                                    etd.cal_mnth_nm,
                                                                                                    CASE
                                                                                                        WHEN (
                                                                                                            (
                                                                                                                row_number() OVER(
                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                ) % (7)::bigint
                                                                                                            ) = 0
                                                                                                        ) THEN (
                                                                                                            row_number() OVER(
                                                                                                                PARTITION BY etd.cal_year
                                                                                                                ORDER BY to_date(etd.cal_date)
                                                                                                            ) / 7
                                                                                                        )
                                                                                                        ELSE (
                                                                                                            (
                                                                                                                row_number() OVER(
                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                ) / 7
                                                                                                            ) + 1
                                                                                                        )
                                                                                                    END AS cal_wk
                                                                                                FROM edw_time_dim etd
                                                                                            ) etdc,
                                                                                            (
                                                                                                SELECT etdcw.cal_year,
                                                                                                    etdcw.cal_mnth_id,
                                                                                                    etdcw.cal_wk,
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY etdcw.cal_year,
                                                                                                        etdcw.cal_mnth_id
                                                                                                        ORDER BY etdcw.cal_year,
                                                                                                            etdcw.cal_mnth_id,
                                                                                                            etdcw.cal_wk
                                                                                                    ) AS cal_mnth_wk
                                                                                                FROM (
                                                                                                        SELECT DISTINCT etdc.cal_year,
                                                                                                            etdc.cal_mnth_id,
                                                                                                            etdc.cal_wk
                                                                                                        FROM (
                                                                                                                SELECT etd.cal_year,
                                                                                                                    etd.cal_mnth_id,
                                                                                                                    CASE
                                                                                                                        WHEN (
                                                                                                                            (
                                                                                                                                row_number() OVER(
                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                                ) % (7)::bigint
                                                                                                                            ) = 0
                                                                                                                        ) THEN (
                                                                                                                            row_number() OVER(
                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                ORDER BY to_date(etd.cal_date)
                                                                                                                            ) / 7
                                                                                                                        )
                                                                                                                        ELSE (
                                                                                                                            (
                                                                                                                                row_number() OVER(
                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                                ) / 7
                                                                                                                            ) + 1
                                                                                                                        )
                                                                                                                    END AS cal_wk
                                                                                                                FROM edw_time_dim etd
                                                                                                            ) etdc
                                                                                                    ) etdcw
                                                                                            ) etdcm
                                                                                        WHERE (
                                                                                                (
                                                                                                    (
                                                                                                        (
                                                                                                            (
                                                                                                                (
                                                                                                                    (etd.jj_year = etdw.jj_year)
                                                                                                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                                )
                                                                                                                AND (etd.jj_wk = etdw.jj_wk)
                                                                                                            )
                                                                                                            AND (to_date(etd.cal_date) = to_date(etdc.cal_date))
                                                                                                        )
                                                                                                        AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                                    )
                                                                                                    AND (etdc.cal_year = etdcm.cal_year)
                                                                                                )
                                                                                                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                            )
                                                                                    ) etd,
                                                                                    (
                                                                                        vw_iri_scan_sales iss
                                                                                        LEFT JOIN (
                                                                                            SELECT edw_material_dim.matl_id,
                                                                                                edw_material_dim.matl_desc,
                                                                                                edw_material_dim.mega_brnd_cd,
                                                                                                edw_material_dim.mega_brnd_desc,
                                                                                                edw_material_dim.brnd_cd,
                                                                                                edw_material_dim.brnd_desc,
                                                                                                edw_material_dim.base_prod_cd,
                                                                                                edw_material_dim.base_prod_desc,
                                                                                                edw_material_dim.variant_cd,
                                                                                                edw_material_dim.variant_desc,
                                                                                                edw_material_dim.fran_cd,
                                                                                                edw_material_dim.fran_desc,
                                                                                                edw_material_dim.grp_fran_cd,
                                                                                                edw_material_dim.grp_fran_desc,
                                                                                                edw_material_dim.matl_type_cd,
                                                                                                edw_material_dim.matl_type_desc,
                                                                                                edw_material_dim.prod_fran_cd,
                                                                                                edw_material_dim.prod_fran_desc,
                                                                                                edw_material_dim.prod_hier_cd,
                                                                                                edw_material_dim.prod_hier_desc,
                                                                                                edw_material_dim.prod_mjr_cd,
                                                                                                edw_material_dim.prod_mjr_desc,
                                                                                                edw_material_dim.prod_mnr_cd,
                                                                                                edw_material_dim.prod_mnr_desc,
                                                                                                edw_material_dim.mercia_plan,
                                                                                                edw_material_dim.putup_cd,
                                                                                                edw_material_dim.putup_desc,
                                                                                                edw_material_dim.bar_cd,
                                                                                                edw_material_dim.updt_dt,
                                                                                                edw_material_dim.prft_ctr
                                                                                            FROM edw_material_dim
                                                                                            WHERE (
                                                                                                    edw_material_dim.bar_cd IN (
                                                                                                        SELECT DISTINCT derived_table1.bar_cd
                                                                                                        FROM (
                                                                                                                SELECT count(*) AS count,
                                                                                                                    edw_material_dim.bar_cd
                                                                                                                FROM edw_material_dim
                                                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                                                HAVING (count(*) = 1)
                                                                                                            ) derived_table1
                                                                                                    )
                                                                                                )
                                                                                        ) ean ON (
                                                                                            (
                                                                                                ltrim(
                                                                                                    (ean.bar_cd)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                ) = ltrim(
                                                                                                    (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                                                    ('0'::character varying)::text
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                WHERE (
                                                                                        to_date((iss.wk_end_dt)::timestamp without time zone) = to_date(etd.cal_date)
                                                                                    )
                                                                            ) sales_derived
                                                                            LEFT JOIN edw_material_dim vmd ON (
                                                                                (
                                                                                    ltrim(
                                                                                        (vmd.matl_id)::text,
                                                                                        ('0'::character varying)::text
                                                                                    ) = ltrim(
                                                                                        (
                                                                                            COALESCE(sales_derived.matl_id, '0'::character varying)
                                                                                        )::text,
                                                                                        ('0'::character varying)::text
                                                                                    )
                                                                                )
                                                                            )
                                                                        )
                                                                        LEFT JOIN vw_customer_dim vcd ON (
                                                                            (
                                                                                ltrim(
                                                                                    (vcd.cust_no)::text,
                                                                                    ('0'::character varying)::text
                                                                                ) = ltrim(
                                                                                    (sales_derived.ac_code)::text,
                                                                                    ('0'::character varying)::text
                                                                                )
                                                                            )
                                                                        )
                                                                    )
                                                                WHERE (
                                                                        sales_derived.matl_bar_cd IN (
                                                                            SELECT DISTINCT derived_table2.bar_cd
                                                                            FROM (
                                                                                    SELECT count(*) AS count,
                                                                                        edw_material_dim.bar_cd
                                                                                    FROM edw_material_dim
                                                                                    GROUP BY edw_material_dim.bar_cd
                                                                                    HAVING (count(*) = 1)
                                                                                ) derived_table2
                                                                        )
                                                                    )
                                                            ) bar_cd_map
                                                        UNION ALL
                                                        SELECT sales_derived.time_id,
                                                            sales_derived.jj_year,
                                                            sales_derived.jj_qrtr,
                                                            sales_derived.jj_mnth,
                                                            sales_derived.jj_wk,
                                                            sales_derived.jj_mnth_wk,
                                                            sales_derived.jj_mnth_id,
                                                            sales_derived.jj_mnth_tot,
                                                            sales_derived.jj_mnth_day,
                                                            sales_derived.jj_mnth_shrt,
                                                            sales_derived.jj_mnth_long,
                                                            sales_derived.cal_year,
                                                            sales_derived.cal_qrtr,
                                                            sales_derived.cal_mnth,
                                                            sales_derived.cal_wk,
                                                            sales_derived.cal_mnth_wk,
                                                            sales_derived.cal_mnth_id,
                                                            sales_derived.cal_mnth_nm,
                                                            sales_derived.wk_end_dt,
                                                            sales_derived.ac_code AS ac_attribute,
                                                            (vcd.cust_nm)::character varying AS cust_nm,
                                                            vcd.channel_cd,
                                                            vcd.channel_desc,
                                                            sales_derived.sales_grp_cd,
                                                            sales_derived.sales_grp_desc,
                                                            vcd.cmp_id,
                                                            vcd.country,
                                                            sales_derived.iri_market,
                                                            sales_derived.ac_nielsencode,
                                                            sales_derived.ac_longname,
                                                            sales_derived.iri_ean,
                                                            NULL::character varying AS matl_bar_cd,
                                                            sales_derived.iri_prod_desc,
                                                            (
                                                                ltrim(
                                                                    (vmd.matl_id)::text,
                                                                    ('0'::character varying)::text
                                                                )
                                                            )::character varying AS matl_id,
                                                            vmd.matl_desc,
                                                            vmd.brnd_cd,
                                                            vmd.brnd_desc,
                                                            vmd.fran_cd,
                                                            vmd.fran_desc,
                                                            vmd.grp_fran_cd,
                                                            vmd.grp_fran_desc,
                                                            vmd.prod_fran_cd,
                                                            vmd.prod_fran_desc,
                                                            vmd.prod_mjr_cd,
                                                            vmd.prod_mjr_desc,
                                                            vmd.prod_mnr_cd,
                                                            vmd.prod_mnr_desc,
                                                            sales_derived.scan_units,
                                                            sales_derived.scan_sales
                                                        FROM (
                                                                (
                                                                    (
                                                                        SELECT iss.iri_market,
                                                                            iss.wk_end_dt,
                                                                            iss.iri_prod_desc,
                                                                            iss.iri_ean,
                                                                            iss.ac_nielsencode,
                                                                            iss.ac_code,
                                                                            iss.ac_longname,
                                                                            iss.sales_grp_cd,
                                                                            iss.sales_grp_desc,
                                                                            iss.scan_sales,
                                                                            iss.scan_units,
                                                                            etd.cal_date,
                                                                            etd.time_id,
                                                                            etd.jj_wk,
                                                                            etd.jj_mnth,
                                                                            etd.jj_mnth_shrt,
                                                                            etd.jj_mnth_long,
                                                                            etd.jj_qrtr,
                                                                            etd.jj_year,
                                                                            etd.cal_mnth_id,
                                                                            etd.jj_mnth_id,
                                                                            etd.cal_mnth,
                                                                            etd.cal_qrtr,
                                                                            etd.cal_year,
                                                                            etd.jj_mnth_tot,
                                                                            etd.jj_mnth_day,
                                                                            etd.cal_mnth_nm,
                                                                            etd.jj_mnth_wk,
                                                                            etd.cal_wk,
                                                                            etd.cal_mnth_wk
                                                                        FROM (
                                                                                SELECT etd.cal_date,
                                                                                    etd.time_id,
                                                                                    etd.jj_wk,
                                                                                    etd.jj_mnth,
                                                                                    etd.jj_mnth_shrt,
                                                                                    etd.jj_mnth_long,
                                                                                    etd.jj_qrtr,
                                                                                    etd.jj_year,
                                                                                    etd.cal_mnth_id,
                                                                                    etd.jj_mnth_id,
                                                                                    etd.cal_mnth,
                                                                                    etd.cal_qrtr,
                                                                                    etd.cal_year,
                                                                                    etd.jj_mnth_tot,
                                                                                    etd.jj_mnth_day,
                                                                                    etd.cal_mnth_nm,
                                                                                    etdw.jj_mnth_wk,
                                                                                    etdc.cal_wk,
                                                                                    etdcm.cal_mnth_wk
                                                                                FROM edw_time_dim etd,
                                                                                    (
                                                                                        SELECT etd.jj_year,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.jj_wk,
                                                                                            row_number() OVER(
                                                                                                PARTITION BY etd.jj_year,
                                                                                                etd.jj_mnth_id
                                                                                                ORDER BY etd.jj_year,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.jj_wk
                                                                                            ) AS jj_mnth_wk
                                                                                        FROM (
                                                                                                SELECT DISTINCT etd.jj_year,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.jj_wk
                                                                                                FROM edw_time_dim etd
                                                                                            ) etd
                                                                                    ) etdw,
                                                                                    (
                                                                                        SELECT etd.cal_date,
                                                                                            etd.time_id,
                                                                                            etd.jj_wk,
                                                                                            etd.jj_mnth,
                                                                                            etd.jj_mnth_shrt,
                                                                                            etd.jj_mnth_long,
                                                                                            etd.jj_qrtr,
                                                                                            etd.jj_year,
                                                                                            etd.cal_mnth_id,
                                                                                            etd.jj_mnth_id,
                                                                                            etd.cal_mnth,
                                                                                            etd.cal_qrtr,
                                                                                            etd.cal_year,
                                                                                            etd.jj_mnth_tot,
                                                                                            etd.jj_mnth_day,
                                                                                            etd.cal_mnth_nm,
                                                                                            CASE
                                                                                                WHEN (
                                                                                                    (
                                                                                                        row_number() OVER(
                                                                                                            PARTITION BY etd.cal_year
                                                                                                            ORDER BY to_date(etd.cal_date)
                                                                                                        ) % (7)::bigint
                                                                                                    ) = 0
                                                                                                ) THEN (
                                                                                                    row_number() OVER(
                                                                                                        PARTITION BY etd.cal_year
                                                                                                        ORDER BY to_date(etd.cal_date)
                                                                                                    ) / 7
                                                                                                )
                                                                                                ELSE (
                                                                                                    (
                                                                                                        row_number() OVER(
                                                                                                            PARTITION BY etd.cal_year
                                                                                                            ORDER BY to_date(etd.cal_date)
                                                                                                        ) / 7
                                                                                                    ) + 1
                                                                                                )
                                                                                            END AS cal_wk
                                                                                        FROM edw_time_dim etd
                                                                                    ) etdc,
                                                                                    (
                                                                                        SELECT etdcw.cal_year,
                                                                                            etdcw.cal_mnth_id,
                                                                                            etdcw.cal_wk,
                                                                                            row_number() OVER(
                                                                                                PARTITION BY etdcw.cal_year,
                                                                                                etdcw.cal_mnth_id
                                                                                                ORDER BY etdcw.cal_year,
                                                                                                    etdcw.cal_mnth_id,
                                                                                                    etdcw.cal_wk
                                                                                            ) AS cal_mnth_wk
                                                                                        FROM (
                                                                                                SELECT DISTINCT etdc.cal_year,
                                                                                                    etdc.cal_mnth_id,
                                                                                                    etdc.cal_wk
                                                                                                FROM (
                                                                                                        SELECT etd.cal_year,
                                                                                                            etd.cal_mnth_id,
                                                                                                            CASE
                                                                                                                WHEN (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY to_date(etd.cal_date)
                                                                                                                        ) % (7)::bigint
                                                                                                                    ) = 0
                                                                                                                ) THEN (
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etd.cal_year
                                                                                                                        ORDER BY to_date(etd.cal_date)
                                                                                                                    ) / 7
                                                                                                                )
                                                                                                                ELSE (
                                                                                                                    (
                                                                                                                        row_number() OVER(
                                                                                                                            PARTITION BY etd.cal_year
                                                                                                                            ORDER BY to_date(etd.cal_date)
                                                                                                                        ) / 7
                                                                                                                    ) + 1
                                                                                                                )
                                                                                                            END AS cal_wk
                                                                                                        FROM edw_time_dim etd
                                                                                                    ) etdc
                                                                                            ) etdcw
                                                                                    ) etdcm
                                                                                WHERE (
                                                                                        (
                                                                                            (
                                                                                                (
                                                                                                    (
                                                                                                        (
                                                                                                            (etd.jj_year = etdw.jj_year)
                                                                                                            AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                        )
                                                                                                        AND (etd.jj_wk = etdw.jj_wk)
                                                                                                    )
                                                                                                    AND (to_date(etd.cal_date) = to_date(etdc.cal_date))
                                                                                                )
                                                                                                AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                            )
                                                                                            AND (etdc.cal_year = etdcm.cal_year)
                                                                                        )
                                                                                        AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                    )
                                                                            ) etd,
                                                                            vw_iri_scan_sales iss
                                                                        WHERE (
                                                                                to_date((iss.wk_end_dt)::timestamp without time zone) = to_date(etd.cal_date)
                                                                            )
                                                                    ) sales_derived
                                                                    LEFT JOIN (
                                                                        (
                                                                            SELECT derived_table6.jj_month_id,
                                                                                derived_table6.bar_cd,
                                                                                derived_table6.cust_no,
                                                                                derived_table6.material_id
                                                                            FROM (
                                                                                    SELECT DISTINCT derived_table5.jj_month_id,
                                                                                        derived_table5.bar_cd,
                                                                                        derived_table5.cust_no,
                                                                                        derived_table5.material_id
                                                                                    FROM (
                                                                                            SELECT DISTINCT derived_table4.jj_month_id,
                                                                                                derived_table4.master_code,
                                                                                                derived_table4.bar_cd,
                                                                                                derived_table4.cust_no,
                                                                                                derived_table4.material_count,
                                                                                                derived_table4.gts_val,
                                                                                                count(DISTINCT derived_table4.master_code) AS count,
                                                                                                CASE
                                                                                                    WHEN (count(DISTINCT derived_table4.master_code) > 1) THEN CASE
                                                                                                        WHEN (
                                                                                                            (
                                                                                                                (derived_table4.master_code IS NOT NULL)
                                                                                                                AND (
                                                                                                                    derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                                                                                )
                                                                                                            )
                                                                                                            AND (
                                                                                                                upper((derived_table4.channel_desc)::text) <> ('AU - EXPORTS'::character varying)::text
                                                                                                            )
                                                                                                        ) THEN (derived_table4.max_material_id)::character varying
                                                                                                        WHEN (
                                                                                                            (
                                                                                                                (derived_table4.master_code IS NULL)
                                                                                                                AND (
                                                                                                                    derived_table4.gts_val < ((0)::numeric)::numeric(18, 0)
                                                                                                                )
                                                                                                            )
                                                                                                            AND (
                                                                                                                upper((derived_table4.channel_desc)::text) = ('AU - EXPORTS'::character varying)::text
                                                                                                            )
                                                                                                        ) THEN 'NULL'::character varying
                                                                                                        ELSE NULL::character varying
                                                                                                    END
                                                                                                    WHEN (count(DISTINCT derived_table4.master_code) = 1) THEN CASE
                                                                                                        WHEN (
                                                                                                            (derived_table4.material_count > 1)
                                                                                                            AND (
                                                                                                                derived_table4.gts_val >= ((0)::numeric)::numeric(18, 0)
                                                                                                            )
                                                                                                        ) THEN (derived_table4.max_material_id)::character varying
                                                                                                        WHEN (derived_table4.material_count = 1) THEN (derived_table4.max_material_id)::character varying
                                                                                                        ELSE NULL::character varying
                                                                                                    END
                                                                                                    ELSE derived_table4.material_id
                                                                                                END AS material_id
                                                                                            FROM (
                                                                                                    SELECT DISTINCT a.jj_month_id,
                                                                                                        a.master_code,
                                                                                                        a.bar_cd,
                                                                                                        a.matl_id AS material_id,
                                                                                                        a.cust_no,
                                                                                                        count(a.matl_id) OVER(
                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                            a.bar_cd,
                                                                                                            a.cust_no 
                                                                                                            order by null
                                                                                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                        ) AS material_count,
                                                                                                        "max"((a.matl_id)::text) OVER(
                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                            a.master_code,
                                                                                                            a.bar_cd,
                                                                                                            a.cust_no,
                                                                                                            sum(a.gts_val) 
                                                                                                            order by null
                                                                                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                        ) AS max_material_id,
                                                                                                        row_number() OVER(
                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                            a.bar_cd,
                                                                                                            a.cust_no
                                                                                                            ORDER BY sum(a.gts_val) DESC
                                                                                                        ) AS sales_rank,
                                                                                                        count(COALESCE(a.master_code, 'NA'::character varying)) OVER(
                                                                                                            PARTITION BY a.jj_month_id,
                                                                                                            a.bar_cd,
                                                                                                            a.cust_no,
                                                                                                            a.matl_id 
                                                                                                            order by null
                                                                                                            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                                                                                                        ) AS master_code_count,
                                                                                                        sum(a.gts_val) AS gts_val,
                                                                                                        a.channel_desc,
                                                                                                        b.matl_bar_count
                                                                                                    FROM (
                                                                                                            (
                                                                                                                SELECT vsf.jj_month_id,
                                                                                                                    vsf.gts_val,
                                                                                                                    vmd.matl_id,
                                                                                                                    vmd.bar_cd,
                                                                                                                    mstrcd.master_code,
                                                                                                                    vcd.cust_no,
                                                                                                                    vcd.channel_desc
                                                                                                                FROM (
                                                                                                                        (
                                                                                                                            (
                                                                                                                                vw_sapbw_ciw_fact vsf
                                                                                                                                LEFT JOIN vw_customer_dim vcd ON (((vsf.cust_no)::text = (vcd.cust_no)::text))
                                                                                                                            )
                                                                                                                            LEFT JOIN edw_material_dim vmd ON (((vsf.matl_id)::text = (vmd.matl_id)::text))
                                                                                                                        )
                                                                                                                        LEFT JOIN (
                                                                                                                            vw_apo_parent_child_dim vapcd
                                                                                                                            LEFT JOIN (
                                                                                                                                SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                                                                                    vw_apo_parent_child_dim.parent_matl_desc
                                                                                                                                FROM vw_apo_parent_child_dim
                                                                                                                                WHERE (
                                                                                                                                        (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                                                                                                    )
                                                                                                                                UNION ALL
                                                                                                                                SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                                                                                    vw_apo_parent_child_dim.parent_matl_desc
                                                                                                                                FROM vw_apo_parent_child_dim
                                                                                                                                WHERE (
                                                                                                                                        NOT (
                                                                                                                                            vw_apo_parent_child_dim.master_code IN (
                                                                                                                                                SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                                                                                                                                FROM vw_apo_parent_child_dim
                                                                                                                                                WHERE (
                                                                                                                                                        (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                                                                                                                    )
                                                                                                                                            )
                                                                                                                                        )
                                                                                                                                    )
                                                                                                                            ) mstrcd ON (
                                                                                                                                (
                                                                                                                                    (vapcd.master_code)::text = (mstrcd.master_code)::text
                                                                                                                                )
                                                                                                                            )
                                                                                                                        ) ON (
                                                                                                                            (
                                                                                                                                ((vsf.cmp_id)::text = (vapcd.cmp_id)::text)
                                                                                                                                AND ((vsf.matl_id)::text = (vapcd.matl_id)::text)
                                                                                                                            )
                                                                                                                        )
                                                                                                                    )
                                                                                                            ) a
                                                                                                            JOIN (
                                                                                                                SELECT DISTINCT edw_material_dim.bar_cd,
                                                                                                                    count(DISTINCT edw_material_dim.matl_id) AS matl_bar_count
                                                                                                                FROM edw_material_dim
                                                                                                                WHERE (
                                                                                                                        COALESCE(edw_material_dim.bar_cd, 'NA'::character varying) IN (
                                                                                                                            SELECT DISTINCT COALESCE(derived_table3.bar_cd, 'NA'::character varying) AS "coalesce"
                                                                                                                            FROM (
                                                                                                                                    SELECT count(*) AS count,
                                                                                                                                        edw_material_dim.bar_cd
                                                                                                                                    FROM edw_material_dim
                                                                                                                                    GROUP BY edw_material_dim.bar_cd
                                                                                                                                    HAVING (count(*) > 1)
                                                                                                                                ) derived_table3
                                                                                                                        )
                                                                                                                    )
                                                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                                            ) b ON (((a.bar_cd)::text = (b.bar_cd)::text))
                                                                                                        )
                                                                                                    GROUP BY a.jj_month_id,
                                                                                                        a.master_code,
                                                                                                        a.bar_cd,
                                                                                                        a.matl_id,
                                                                                                        a.cust_no,
                                                                                                        a.channel_desc,
                                                                                                        b.matl_bar_count
                                                                                                ) derived_table4
                                                                                            WHERE (derived_table4.sales_rank = 1)
                                                                                            GROUP BY derived_table4.jj_month_id,
                                                                                                derived_table4.master_code,
                                                                                                derived_table4.bar_cd,
                                                                                                derived_table4.cust_no,
                                                                                                derived_table4.material_id,
                                                                                                derived_table4.material_count,
                                                                                                derived_table4.max_material_id,
                                                                                                derived_table4.master_code_count,
                                                                                                derived_table4.channel_desc,
                                                                                                derived_table4.gts_val,
                                                                                                derived_table4.matl_bar_count
                                                                                        ) derived_table5
                                                                                ) derived_table6
                                                                        ) ean
                                                                        LEFT JOIN edw_material_dim vmd ON (
                                                                            (
                                                                                ltrim(
                                                                                    (vmd.matl_id)::text,
                                                                                    ('0'::character varying)::text
                                                                                ) = ltrim(
                                                                                    (
                                                                                        COALESCE(ean.material_id, '0'::character varying)
                                                                                    )::text,
                                                                                    ('0'::character varying)::text
                                                                                )
                                                                            )
                                                                        )
                                                                    ) ON (
                                                                        (
                                                                            (
                                                                                (
                                                                                    ((ean.jj_month_id)::numeric)::numeric(18, 0) = sales_derived.jj_mnth_id
                                                                                )
                                                                                AND (
                                                                                    ltrim(
                                                                                        (ean.cust_no)::text,
                                                                                        ('0'::character varying)::text
                                                                                    ) = (sales_derived.ac_code)::text
                                                                                )
                                                                            )
                                                                            AND (
                                                                                (COALESCE(ean.bar_cd, '0'::character varying))::text = (
                                                                                    COALESCE(sales_derived.iri_ean, '0'::character varying)
                                                                                )::text
                                                                            )
                                                                        )
                                                                    )
                                                                )
                                                                LEFT JOIN vw_customer_dim vcd ON (
                                                                    (
                                                                        ltrim(
                                                                            (vcd.cust_no)::text,
                                                                            ('0'::character varying)::text
                                                                        ) = ltrim(
                                                                            (sales_derived.ac_code)::text,
                                                                            ('0'::character varying)::text
                                                                        )
                                                                    )
                                                                )
                                                            )
                                                        WHERE (
                                                                NOT (
                                                                    COALESCE(sales_derived.iri_ean, 'NA'::character varying) IN (
                                                                        SELECT DISTINCT COALESCE(bar_cd_map.iri_ean, '0'::character varying) AS "coalesce"
                                                                        FROM (
                                                                                SELECT sales_derived.time_id,
                                                                                    sales_derived.jj_year,
                                                                                    sales_derived.jj_qrtr,
                                                                                    sales_derived.jj_mnth,
                                                                                    sales_derived.jj_wk,
                                                                                    sales_derived.jj_mnth_wk,
                                                                                    sales_derived.jj_mnth_id,
                                                                                    sales_derived.jj_mnth_tot,
                                                                                    sales_derived.jj_mnth_day,
                                                                                    sales_derived.jj_mnth_shrt,
                                                                                    sales_derived.jj_mnth_long,
                                                                                    sales_derived.cal_year,
                                                                                    sales_derived.cal_qrtr,
                                                                                    sales_derived.cal_mnth,
                                                                                    sales_derived.cal_wk,
                                                                                    sales_derived.cal_mnth_wk,
                                                                                    sales_derived.cal_mnth_id,
                                                                                    sales_derived.cal_mnth_nm,
                                                                                    sales_derived.wk_end_dt,
                                                                                    sales_derived.ac_code AS ac_attribute,
                                                                                    vcd.cust_nm,
                                                                                    vcd.channel_cd,
                                                                                    vcd.channel_desc,
                                                                                    sales_derived.sales_grp_cd,
                                                                                    sales_derived.sales_grp_desc,
                                                                                    vcd.cmp_id,
                                                                                    vcd.country,
                                                                                    sales_derived.iri_market,
                                                                                    sales_derived.ac_nielsencode,
                                                                                    sales_derived.ac_longname,
                                                                                    sales_derived.iri_ean,
                                                                                    sales_derived.matl_bar_cd,
                                                                                    sales_derived.iri_prod_desc,
                                                                                    ltrim(
                                                                                        (vmd.matl_id)::text,
                                                                                        ('0'::character varying)::text
                                                                                    ) AS matl_id,
                                                                                    vmd.matl_desc,
                                                                                    vmd.brnd_cd,
                                                                                    vmd.brnd_desc,
                                                                                    vmd.fran_cd,
                                                                                    vmd.fran_desc,
                                                                                    vmd.grp_fran_cd,
                                                                                    vmd.grp_fran_desc,
                                                                                    vmd.prod_fran_cd,
                                                                                    vmd.prod_fran_desc,
                                                                                    vmd.prod_mjr_cd,
                                                                                    vmd.prod_mjr_desc,
                                                                                    vmd.prod_mnr_cd,
                                                                                    vmd.prod_mnr_desc,
                                                                                    sales_derived.scan_units,
                                                                                    sales_derived.scan_sales
                                                                                FROM (
                                                                                        (
                                                                                            (
                                                                                                SELECT iss.iri_market,
                                                                                                    iss.wk_end_dt,
                                                                                                    iss.iri_prod_desc,
                                                                                                    iss.iri_ean,
                                                                                                    iss.ac_nielsencode,
                                                                                                    iss.ac_code,
                                                                                                    iss.ac_longname,
                                                                                                    iss.sales_grp_cd,
                                                                                                    iss.sales_grp_desc,
                                                                                                    iss.scan_sales,
                                                                                                    iss.scan_units,
                                                                                                    etd.cal_date,
                                                                                                    etd.time_id,
                                                                                                    etd.jj_wk,
                                                                                                    etd.jj_mnth,
                                                                                                    etd.jj_mnth_shrt,
                                                                                                    etd.jj_mnth_long,
                                                                                                    etd.jj_qrtr,
                                                                                                    etd.jj_year,
                                                                                                    etd.cal_mnth_id,
                                                                                                    etd.jj_mnth_id,
                                                                                                    etd.cal_mnth,
                                                                                                    etd.cal_qrtr,
                                                                                                    etd.cal_year,
                                                                                                    etd.jj_mnth_tot,
                                                                                                    etd.jj_mnth_day,
                                                                                                    etd.cal_mnth_nm,
                                                                                                    etd.jj_mnth_wk,
                                                                                                    etd.cal_wk,
                                                                                                    etd.cal_mnth_wk,
                                                                                                    ean.matl_id,
                                                                                                    ean.bar_cd AS matl_bar_cd
                                                                                                FROM (
                                                                                                        SELECT etd.cal_date,
                                                                                                            etd.time_id,
                                                                                                            etd.jj_wk,
                                                                                                            etd.jj_mnth,
                                                                                                            etd.jj_mnth_shrt,
                                                                                                            etd.jj_mnth_long,
                                                                                                            etd.jj_qrtr,
                                                                                                            etd.jj_year,
                                                                                                            etd.cal_mnth_id,
                                                                                                            etd.jj_mnth_id,
                                                                                                            etd.cal_mnth,
                                                                                                            etd.cal_qrtr,
                                                                                                            etd.cal_year,
                                                                                                            etd.jj_mnth_tot,
                                                                                                            etd.jj_mnth_day,
                                                                                                            etd.cal_mnth_nm,
                                                                                                            etdw.jj_mnth_wk,
                                                                                                            etdc.cal_wk,
                                                                                                            etdcm.cal_mnth_wk
                                                                                                        FROM edw_time_dim etd,
                                                                                                            (
                                                                                                                SELECT etd.jj_year,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.jj_wk,
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etd.jj_year,
                                                                                                                        etd.jj_mnth_id
                                                                                                                        ORDER BY etd.jj_year,
                                                                                                                            etd.jj_mnth_id,
                                                                                                                            etd.jj_wk
                                                                                                                    ) AS jj_mnth_wk
                                                                                                                FROM (
                                                                                                                        SELECT DISTINCT etd.jj_year,
                                                                                                                            etd.jj_mnth_id,
                                                                                                                            etd.jj_wk
                                                                                                                        FROM edw_time_dim etd
                                                                                                                    ) etd
                                                                                                            ) etdw,
                                                                                                            (
                                                                                                                SELECT etd.cal_date,
                                                                                                                    etd.time_id,
                                                                                                                    etd.jj_wk,
                                                                                                                    etd.jj_mnth,
                                                                                                                    etd.jj_mnth_shrt,
                                                                                                                    etd.jj_mnth_long,
                                                                                                                    etd.jj_qrtr,
                                                                                                                    etd.jj_year,
                                                                                                                    etd.cal_mnth_id,
                                                                                                                    etd.jj_mnth_id,
                                                                                                                    etd.cal_mnth,
                                                                                                                    etd.cal_qrtr,
                                                                                                                    etd.cal_year,
                                                                                                                    etd.jj_mnth_tot,
                                                                                                                    etd.jj_mnth_day,
                                                                                                                    etd.cal_mnth_nm,
                                                                                                                    CASE
                                                                                                                        WHEN (
                                                                                                                            (
                                                                                                                                row_number() OVER(
                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                                ) % (7)::bigint
                                                                                                                            ) = 0
                                                                                                                        ) THEN (
                                                                                                                            row_number() OVER(
                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                ORDER BY to_date(etd.cal_date)
                                                                                                                            ) / 7
                                                                                                                        )
                                                                                                                        ELSE (
                                                                                                                            (
                                                                                                                                row_number() OVER(
                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                                ) / 7
                                                                                                                            ) + 1
                                                                                                                        )
                                                                                                                    END AS cal_wk
                                                                                                                FROM edw_time_dim etd
                                                                                                            ) etdc,
                                                                                                            (
                                                                                                                SELECT etdcw.cal_year,
                                                                                                                    etdcw.cal_mnth_id,
                                                                                                                    etdcw.cal_wk,
                                                                                                                    row_number() OVER(
                                                                                                                        PARTITION BY etdcw.cal_year,
                                                                                                                        etdcw.cal_mnth_id
                                                                                                                        ORDER BY etdcw.cal_year,
                                                                                                                            etdcw.cal_mnth_id,
                                                                                                                            etdcw.cal_wk
                                                                                                                    ) AS cal_mnth_wk
                                                                                                                FROM (
                                                                                                                        SELECT DISTINCT etdc.cal_year,
                                                                                                                            etdc.cal_mnth_id,
                                                                                                                            etdc.cal_wk
                                                                                                                        FROM (
                                                                                                                                SELECT etd.cal_year,
                                                                                                                                    etd.cal_mnth_id,
                                                                                                                                    CASE
                                                                                                                                        WHEN (
                                                                                                                                            (
                                                                                                                                                row_number() OVER(
                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                                                ) % (7)::bigint
                                                                                                                                            ) = 0
                                                                                                                                        ) THEN (
                                                                                                                                            row_number() OVER(
                                                                                                                                                PARTITION BY etd.cal_year
                                                                                                                                                ORDER BY to_date(etd.cal_date)
                                                                                                                                            ) / 7
                                                                                                                                        )
                                                                                                                                        ELSE (
                                                                                                                                            (
                                                                                                                                                row_number() OVER(
                                                                                                                                                    PARTITION BY etd.cal_year
                                                                                                                                                    ORDER BY to_date(etd.cal_date)
                                                                                                                                                ) / 7
                                                                                                                                            ) + 1
                                                                                                                                        )
                                                                                                                                    END AS cal_wk
                                                                                                                                FROM edw_time_dim etd
                                                                                                                            ) etdc
                                                                                                                    ) etdcw
                                                                                                            ) etdcm
                                                                                                        WHERE (
                                                                                                                (
                                                                                                                    (
                                                                                                                        (
                                                                                                                            (
                                                                                                                                (
                                                                                                                                    (etd.jj_year = etdw.jj_year)
                                                                                                                                    AND (etd.jj_mnth_id = etdw.jj_mnth_id)
                                                                                                                                )
                                                                                                                                AND (etd.jj_wk = etdw.jj_wk)
                                                                                                                            )
                                                                                                                            AND (to_date(etd.cal_date) = to_date(etdc.cal_date))
                                                                                                                        )
                                                                                                                        AND (etdc.cal_wk = etdcm.cal_wk)
                                                                                                                    )
                                                                                                                    AND (etdc.cal_year = etdcm.cal_year)
                                                                                                                )
                                                                                                                AND (etdc.cal_mnth_id = etdcm.cal_mnth_id)
                                                                                                            )
                                                                                                    ) etd,
                                                                                                    (
                                                                                                        vw_iri_scan_sales iss
                                                                                                        LEFT JOIN (
                                                                                                            SELECT edw_material_dim.matl_id,
                                                                                                                edw_material_dim.matl_desc,
                                                                                                                edw_material_dim.mega_brnd_cd,
                                                                                                                edw_material_dim.mega_brnd_desc,
                                                                                                                edw_material_dim.brnd_cd,
                                                                                                                edw_material_dim.brnd_desc,
                                                                                                                edw_material_dim.base_prod_cd,
                                                                                                                edw_material_dim.base_prod_desc,
                                                                                                                edw_material_dim.variant_cd,
                                                                                                                edw_material_dim.variant_desc,
                                                                                                                edw_material_dim.fran_cd,
                                                                                                                edw_material_dim.fran_desc,
                                                                                                                edw_material_dim.grp_fran_cd,
                                                                                                                edw_material_dim.grp_fran_desc,
                                                                                                                edw_material_dim.matl_type_cd,
                                                                                                                edw_material_dim.matl_type_desc,
                                                                                                                edw_material_dim.prod_fran_cd,
                                                                                                                edw_material_dim.prod_fran_desc,
                                                                                                                edw_material_dim.prod_hier_cd,
                                                                                                                edw_material_dim.prod_hier_desc,
                                                                                                                edw_material_dim.prod_mjr_cd,
                                                                                                                edw_material_dim.prod_mjr_desc,
                                                                                                                edw_material_dim.prod_mnr_cd,
                                                                                                                edw_material_dim.prod_mnr_desc,
                                                                                                                edw_material_dim.mercia_plan,
                                                                                                                edw_material_dim.putup_cd,
                                                                                                                edw_material_dim.putup_desc,
                                                                                                                edw_material_dim.bar_cd,
                                                                                                                edw_material_dim.updt_dt,
                                                                                                                edw_material_dim.prft_ctr
                                                                                                            FROM edw_material_dim
                                                                                                            WHERE (
                                                                                                                    edw_material_dim.bar_cd IN (
                                                                                                                        SELECT DISTINCT derived_table1.bar_cd
                                                                                                                        FROM (
                                                                                                                                SELECT count(*) AS count,
                                                                                                                                    edw_material_dim.bar_cd
                                                                                                                                FROM edw_material_dim
                                                                                                                                GROUP BY edw_material_dim.bar_cd
                                                                                                                                HAVING (count(*) = 1)
                                                                                                                            ) derived_table1
                                                                                                                    )
                                                                                                                )
                                                                                                        ) ean ON (
                                                                                                            (
                                                                                                                ltrim(
                                                                                                                    (ean.bar_cd)::text,
                                                                                                                    ('0'::character varying)::text
                                                                                                                ) = ltrim(
                                                                                                                    (COALESCE(iss.iri_ean, '0'::character varying))::text,
                                                                                                                    ('0'::character varying)::text
                                                                                                                )
                                                                                                            )
                                                                                                        )
                                                                                                    )
                                                                                                WHERE (
                                                                                                        to_date((iss.wk_end_dt)::timestamp without time zone) = to_date(etd.cal_date)
                                                                                                    )
                                                                                            ) sales_derived
                                                                                            LEFT JOIN edw_material_dim vmd ON (
                                                                                                (
                                                                                                    ltrim(
                                                                                                        (vmd.matl_id)::text,
                                                                                                        ('0'::character varying)::text
                                                                                                    ) = ltrim(
                                                                                                        (
                                                                                                            COALESCE(sales_derived.matl_id, '0'::character varying)
                                                                                                        )::text,
                                                                                                        ('0'::character varying)::text
                                                                                                    )
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                        LEFT JOIN vw_customer_dim vcd ON (
                                                                                            (
                                                                                                ltrim(
                                                                                                    (vcd.cust_no)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                ) = ltrim(
                                                                                                    (sales_derived.ac_code)::text,
                                                                                                    ('0'::character varying)::text
                                                                                                )
                                                                                            )
                                                                                        )
                                                                                    )
                                                                                WHERE (
                                                                                        sales_derived.matl_bar_cd IN (
                                                                                            SELECT DISTINCT derived_table2.bar_cd
                                                                                            FROM (
                                                                                                    SELECT count(*) AS count,
                                                                                                        edw_material_dim.bar_cd
                                                                                                    FROM edw_material_dim
                                                                                                    GROUP BY edw_material_dim.bar_cd
                                                                                                    HAVING (count(*) = 1)
                                                                                                ) derived_table2
                                                                                        )
                                                                                    )
                                                                            ) bar_cd_map
                                                                    )
                                                                )
                                                            )
                                                    ) sales
                                                    LEFT JOIN (
                                                        SELECT vapcd.sales_org,
                                                            vapcd.cmp_id,
                                                            vapcd.matl_id,
                                                            vapcd.matl_desc,
                                                            vapcd.master_code,
                                                            vapcd.launch_date,
                                                            vapcd.predessor_id,
                                                            vapcd.parent_id,
                                                            vapcd.parent_matl_desc
                                                        FROM (
                                                                vw_apo_parent_child_dim vapcd
                                                                LEFT JOIN (
                                                                    SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                        vw_apo_parent_child_dim.parent_matl_desc
                                                                    FROM vw_apo_parent_child_dim
                                                                    WHERE (
                                                                            (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                                        )
                                                                    UNION ALL
                                                                    SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                                                        vw_apo_parent_child_dim.parent_matl_desc
                                                                    FROM vw_apo_parent_child_dim
                                                                    WHERE (
                                                                            NOT (
                                                                                vw_apo_parent_child_dim.master_code IN (
                                                                                    SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                                                                    FROM vw_apo_parent_child_dim
                                                                                    WHERE (
                                                                                            (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                                                        )
                                                                                )
                                                                            )
                                                                        )
                                                                ) mstr ON (
                                                                    (
                                                                        (vapcd.master_code)::text = (mstr.master_code)::text
                                                                    )
                                                                )
                                                            )
                                                    ) matl ON (
                                                        (
                                                            ((matl.cmp_id)::text = (sales.cmp_id)::text)
                                                            AND (
                                                                ltrim(
                                                                    (matl.matl_id)::text,
                                                                    ('0'::character varying)::text
                                                                ) = ltrim(
                                                                    (sales.matl_id)::text,
                                                                    ('0'::character varying)::text
                                                                )
                                                            )
                                                        )
                                                    )
                                                )
                                        ) iri_scan_sales_analysis_cte
                                ) a
                                LEFT JOIN (
                                    SELECT derived_table1.matl_num,
                                        derived_table1.crt_on,
                                        derived_table1.pka_product_key,
                                        derived_table1.pka_product_key_description,
                                        derived_table1.gph_region,
                                        derived_table1.gph_reg_frnchse,
                                        derived_table1.gph_reg_frnchse_grp,
                                        derived_table1.gph_prod_frnchse,
                                        derived_table1.gph_prod_brnd,
                                        derived_table1.gph_prod_sub_brnd,
                                        derived_table1.gph_prod_vrnt,
                                        derived_table1.gph_prod_needstate,
                                        derived_table1.gph_prod_ctgry,
                                        derived_table1.gph_prod_subctgry,
                                        derived_table1.gph_prod_sgmnt,
                                        derived_table1.gph_prod_subsgmnt,
                                        derived_table1.gph_prod_put_up_cd,
                                        derived_table1.gph_prod_put_up_desc,
                                        derived_table1.in_rnk
                                    FROM (
                                            SELECT DISTINCT emd.matl_num,
                                                emd.crt_on,
                                                emd.pka_product_key,
                                                emd.pka_product_key_description,
                                                egph."region" AS gph_region,
                                                egph.regional_franchise AS gph_reg_frnchse,
                                                egph.regional_franchise_group AS gph_reg_frnchse_grp,
                                                egph.gcph_franchise AS gph_prod_frnchse,
                                                egph.gcph_brand AS gph_prod_brnd,
                                                egph.gcph_subbrand AS gph_prod_sub_brnd,
                                                egph.gcph_variant AS gph_prod_vrnt,
                                                egph.gcph_needstate AS gph_prod_needstate,
                                                egph.gcph_category AS gph_prod_ctgry,
                                                egph.gcph_subcategory AS gph_prod_subctgry,
                                                egph.gcph_segment AS gph_prod_sgmnt,
                                                egph.gcph_subsegment AS gph_prod_subsgmnt,
                                                egph.put_up_code AS gph_prod_put_up_cd,
                                                egph.put_up_description AS gph_prod_put_up_desc,
                                                row_number() OVER(
                                                    PARTITION BY emd.matl_num
                                                    ORDER BY emd.matl_num
                                                ) AS in_rnk
                                            FROM (
                                                    rg_edw_material_dim emd
                                                    LEFT JOIN edw_gch_producthierarchy egph ON (
                                                        (
                                                            ltrim(
                                                                (emd.matl_num)::text,
                                                                ((0)::character varying)::text
                                                            ) = ltrim(
                                                                (egph.materialnumber)::text,
                                                                ((0)::character varying)::text
                                                            )
                                                        )
                                                    )
                                                )
                                            WHERE (
                                                    (emd.prod_hier_cd)::text <> (''::character varying)::text
                                                )
                                        ) derived_table1
                                    WHERE (derived_table1.in_rnk = 1)
                                ) b ON (
                                    (
                                        ltrim(a.matl_id, ('0'::character varying)::text) = ltrim(
                                            (b.matl_num)::text,
                                            ('0'::character varying)::text
                                        )
                                    )
                                )
                            )
                    ) derived_table2
                WHERE (derived_table2.rnk = 1)
            ) mat_dim ON 
            (
                (
                    ltrim(
                        (sales_cte.iri_ean)::text,
                        ('0'::character varying)::text
                    ) = ltrim(
                        (mat_dim.iri_ean)::text,
                        ('0'::character varying)::text
                    )
                )
            )
        )
),
final as
(
    SELECT 
        issa.time_id,
        issa.jj_year,
        issa.jj_qrtr,
        issa.jj_mnth,
        issa.jj_wk,
        issa.jj_mnth_wk,
        issa.jj_mnth_id,
        issa.jj_mnth_tot,
        issa.jj_mnth_day,
        issa.jj_mnth_shrt,
        issa.jj_mnth_long,
        issa.cal_year,
        issa.cal_qrtr,
        issa.cal_mnth,
        issa.cal_wk,
        issa.cal_mnth_wk,
        issa.cal_mnth_id,
        issa.cal_mnth_nm,
        issa.wk_end_dt,
        issa.representative_cust_cd,
        issa.representative_cust_nm,
        issa.channel_cd,
        issa.channel_desc,
        issa.country,
        issa.sales_grp_cd,
        issa.sales_grp_nm,
        issa.iri_market,
        issa.ac_nielsencode,
        issa.ac_longname,
        issa.iri_ean,
        issa.iri_prod_desc,
        issa.matl_id,
        issa.matl_desc,
        COALESCE(issa.master_code, ph_mstr.master_code) AS master_code,
        COALESCE(issa.parent_matl_id, ph_mstr.parent_id) AS parent_matl_id,
        COALESCE(issa.parent_matl_desc, ph_mstr.parent_matl_desc) AS parent_matl_desc,
        COALESCE(issa.brnd_cd, ph_emd.brnd_cd) AS brnd_cd,
        COALESCE(issa.brnd_desc, ph_emd.brnd_desc) AS brnd_desc,
        COALESCE(issa.fran_cd, ph_emd.fran_cd) AS fran_cd,
        COALESCE(issa.fran_desc, ph_emd.fran_desc) AS fran_desc,
        COALESCE(issa.grp_fran_cd, ph_emd.grp_fran_cd) AS grp_fran_cd,
        COALESCE(issa.grp_fran_desc, ph_emd.grp_fran_desc) AS grp_fran_desc,
        COALESCE(issa.prod_fran_cd, ph_emd.prod_fran_cd) AS prod_fran_cd,
        COALESCE(issa.prod_fran_desc, ph_emd.prod_fran_desc) AS prod_fran_desc,
        COALESCE(issa.prod_mjr_cd, ph_emd.prod_mjr_cd) AS prod_mjr_cd,
        COALESCE(issa.prod_mjr_desc, ph_emd.prod_mjr_desc) AS prod_mjr_desc,
        COALESCE(issa.prod_mnr_cd, ph_emd.prod_mnr_cd) AS prod_mnr_cd,
        COALESCE(issa.prod_mnr_desc, ph_emd.prod_mnr_desc) AS prod_mnr_desc,
        issa.scan_units,
        issa.scan_sales,
        issa.pka_productkey,
        issa.pka_productdesc,
        issa.lst_sku,
        COALESCE(issa.gph_region, ph_egph.gph_region) AS gph_region,
        COALESCE(issa.gph_reg_frnchse, ph_egph.gph_reg_frnchse) AS gph_reg_frnchse,
        COALESCE(issa.gph_reg_frnchse_grp,ph_egph.gph_reg_frnchse_grp) AS gph_reg_frnchse_grp,
        COALESCE(issa.gph_prod_frnchse, ph_egph.gph_prod_frnchse) AS gph_prod_frnchse,
        COALESCE(issa.gph_prod_brnd, ph_egph.gph_prod_brnd) AS gph_prod_brnd,
        COALESCE(issa.gph_prod_sub_brnd,ph_egph.gph_prod_sub_brnd) AS gph_prod_sub_brnd,
        COALESCE(issa.gph_prod_vrnt, ph_egph.gph_prod_vrnt) AS gph_prod_vrnt,
        COALESCE(issa.gph_prod_needstate,ph_egph.gph_prod_needstate) AS gph_prod_needstate,
        COALESCE(issa.gph_prod_ctgry, ph_egph.gph_prod_ctgry) AS gph_prod_ctgry,
        COALESCE(issa.gph_prod_subctgry,ph_egph.gph_prod_subctgry) AS gph_prod_subctgry,
        COALESCE(issa.gph_prod_sgmnt, ph_egph.gph_prod_sgmnt) AS gph_prod_sgmnt,
        COALESCE(issa.gph_prod_subsgmnt,ph_egph.gph_prod_subsgmnt) AS gph_prod_subsgmnt,
        COALESCE(issa.gph_prod_put_up_cd,ph_egph.gph_prod_put_up_cd) AS gph_prod_put_up_cd,
        COALESCE(issa.gph_prod_put_up_desc,ph_egph.gph_prod_put_up_desc) AS gph_prod_put_up_desc
    FROM issa
    LEFT JOIN 
    (
        SELECT derived_table1.matl_id,
            derived_table1.brnd_cd,
            derived_table1.brnd_desc,
            derived_table1.fran_cd,
            derived_table1.fran_desc,
            derived_table1.grp_fran_cd,
            derived_table1.grp_fran_desc,
            derived_table1.prod_fran_cd,
            derived_table1.prod_fran_desc,
            derived_table1.prod_mjr_cd,
            derived_table1.prod_mjr_desc,
            derived_table1.prod_mnr_cd,
            derived_table1.prod_mnr_desc,
            derived_table1.rnk
        FROM 
        (
                SELECT edw_material_dim.matl_id,
                    edw_material_dim.brnd_cd,
                    edw_material_dim.brnd_desc,
                    edw_material_dim.fran_cd,
                    edw_material_dim.fran_desc,
                    edw_material_dim.grp_fran_cd,
                    edw_material_dim.grp_fran_desc,
                    edw_material_dim.prod_fran_cd,
                    edw_material_dim.prod_fran_desc,
                    edw_material_dim.prod_mjr_cd,
                    edw_material_dim.prod_mjr_desc,
                    edw_material_dim.prod_mnr_cd,
                    edw_material_dim.prod_mnr_desc,
                    row_number() OVER(
                        PARTITION BY ltrim(
                            (edw_material_dim.matl_id)::text,
                            ('0'::character varying)::text
                        )
                        ORDER BY edw_material_dim.brnd_cd
                    ) AS rnk
                FROM edw_material_dim
            ) derived_table1
        WHERE (derived_table1.rnk = 1)
    ) ph_emd ON (
        (
            ltrim
            ((ph_emd.matl_id)::text,('0'::character varying)::text) = 
            ltrim(COALESCE(issa.matl_id, issa.lst_sku),('0'::character varying)::text
            )
        )
    )
    LEFT JOIN 
    (
        SELECT derived_table2.materialnumber,
            derived_table2.gph_region,
            derived_table2.gph_reg_frnchse,
            derived_table2.gph_reg_frnchse_grp,
            derived_table2.gph_prod_frnchse,
            derived_table2.gph_prod_brnd,
            derived_table2.gph_prod_sub_brnd,
            derived_table2.gph_prod_vrnt,
            derived_table2.gph_prod_needstate,
            derived_table2.gph_prod_ctgry,
            derived_table2.gph_prod_subctgry,
            derived_table2.gph_prod_sgmnt,
            derived_table2.gph_prod_subsgmnt,
            derived_table2.gph_prod_put_up_cd,
            derived_table2.gph_prod_put_up_desc,
            derived_table2.rnk
        FROM 
        (
                SELECT egph.materialnumber,
                    egph."region" AS gph_region,
                    egph.regional_franchise AS gph_reg_frnchse,
                    egph.regional_franchise_group AS gph_reg_frnchse_grp,
                    egph.gcph_franchise AS gph_prod_frnchse,
                    egph.gcph_brand AS gph_prod_brnd,
                    egph.gcph_subbrand AS gph_prod_sub_brnd,
                    egph.gcph_variant AS gph_prod_vrnt,
                    egph.gcph_needstate AS gph_prod_needstate,
                    egph.gcph_category AS gph_prod_ctgry,
                    egph.gcph_subcategory AS gph_prod_subctgry,
                    egph.gcph_segment AS gph_prod_sgmnt,
                    egph.gcph_subsegment AS gph_prod_subsgmnt,
                    egph.put_up_code AS gph_prod_put_up_cd,
                    egph.put_up_description AS gph_prod_put_up_desc,
                    row_number() OVER(
                        PARTITION BY ltrim(
                            (egph.materialnumber)::text,
                            ((0)::character varying)::text
                        )
                        ORDER BY egph.gcph_brand
                    ) AS rnk
                FROM edw_gch_producthierarchy egph
            ) derived_table2
        WHERE (derived_table2.rnk = 1)
    ) ph_egph ON 
    (
        (
            ltrim(
                (ph_egph.materialnumber)::text,
                ((0)::character varying)::text
            ) = ltrim(
                COALESCE(issa.matl_id, issa.lst_sku),
                ('0'::character varying)::text
            )
        )
    )
    LEFT JOIN 
    (
        SELECT derived_table3.sales_org,
            derived_table3.cmp_id,
            derived_table3.matl_id,
            derived_table3.matl_desc,
            derived_table3.master_code,
            derived_table3.launch_date,
            derived_table3.predessor_id,
            derived_table3.parent_id,
            derived_table3.parent_matl_desc,
            derived_table3.rnk
        FROM 
            (
                SELECT 
                    vapcd.sales_org,
                    vapcd.cmp_id,
                    vapcd.matl_id,
                    vapcd.matl_desc,
                    vapcd.master_code,
                    vapcd.launch_date,
                    vapcd.predessor_id,
                    ltrim(
                        (vapcd.parent_id)::text,
                        ('0'::character varying)::text
                    ) AS parent_id,
                    vapcd.parent_matl_desc,
                    row_number() OVER(
                        PARTITION BY ltrim(
                            (vapcd.matl_id)::text,
                            ('0'::character varying)::text
                        )
                        ORDER BY ltrim(
                                (vapcd.parent_id)::text,
                                ('0'::character varying)::text
                            ),
                            vapcd.master_code
                    ) AS rnk
                FROM (
                        vw_apo_parent_child_dim vapcd
                        LEFT JOIN (
                            SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                vw_apo_parent_child_dim.parent_matl_desc
                            FROM vw_apo_parent_child_dim
                            WHERE (
                                    (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                )
                            UNION ALL
                            SELECT DISTINCT vw_apo_parent_child_dim.master_code,
                                vw_apo_parent_child_dim.parent_matl_desc
                            FROM vw_apo_parent_child_dim
                            WHERE (
                                    NOT (
                                        vw_apo_parent_child_dim.master_code IN (
                                            SELECT DISTINCT vw_apo_parent_child_dim.master_code
                                            FROM vw_apo_parent_child_dim
                                            WHERE (
                                                    (vw_apo_parent_child_dim.cmp_id)::text = ((7470)::character varying)::text
                                                )
                                        )
                                    )
                                )
                        ) mstr ON (
                            (
                                (vapcd.master_code)::text = (mstr.master_code)::text
                            )
                        )
                    )
            ) derived_table3
        WHERE (derived_table3.rnk = 1)
    ) ph_mstr ON 
    (
        (
            ltrim(
                (ph_mstr.matl_id)::text,
                ((0)::character varying)::text
            ) = ltrim(
                COALESCE(issa.matl_id, issa.lst_sku),
                ('0'::character varying)::text
            )
        )
    )
)
select * from final