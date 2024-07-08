WITH edw_product_key_attributes
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_product_key_attributes') }}
    ),
edw_vw_ps_weights
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_ps_weights') }}
    ),
edw_vw_ps_targets
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_ps_targets') }}
    ),
edw_sku_recom_spike_msl
AS (
    SELECT *
    FROM indedw_integration.edw_sku_recom_spike_msl
    ),
itg_udcdetails
AS (
    SELECT *
    FROM inditg_integration.itg_udcdetails
    ),
edw_calendar_dim
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_calendar_dim') }}
    ),
itg_in_perfectstore_msl
AS (
    SELECT *
    FROM inditg_integration.itg_in_perfectstore_msl
    ),
itg_in_perfectstore_sos
AS (
    SELECT *
    FROM inditg_integration.itg_in_perfectstore_sos
    ),
itg_in_perfectstore_promo
AS (
    SELECT *
    FROM inditg_integration.itg_in_perfectstore_promo
    ),
itg_in_perfectstore_paid_display
AS (
    SELECT *
    FROM inditg_integration.itg_in_perfectstore_paid_display
    ),
ct1
AS (
    SELECT 'Merchandising_Response' AS dataset,
        sku_recom.retailer_cd AS customerid,
        sku_recom.salesman_code AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        CASE 
            WHEN sku_recom.msl_hit = 1
                THEN 'true'::CHARACTER VARYING
            ELSE 'false'::CHARACTER VARYING
            END AS presence,
        NULL AS outofstock,
        'MSL Compliance' AS kpi,
        to_date("substring" (
                sku_recom.year_month::CHARACTER VARYING::TEXT,
                0,
                4
                ) || '-'::CHARACTER VARYING::TEXT || "substring" (
                sku_recom.year_month::CHARACTER VARYING::TEXT,
                5,
                7
                ) || '-15'::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            sku_recom.year_month::CHARACTER VARYING::TEXT,
            0,
            4
            )::CHARACTER VARYING AS fisc_yr,
        sku_recom.year_month AS fisc_per,
        sku_recom.salesman_name AS firstname,
        '' AS lastname,
        (coalesce(sku_recom.retailer_cd, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(sku_recom.retailer_name, 'NA'::CHARACTER VARYING::TEXT))::CHARACTER VARYING AS customername,
        'India' AS country,
        sku_recom.region_name AS STATE,
        sku_recom.customer_name::CHARACTER VARYING AS storereference,
        sku_recom.retailer_category_name AS storetype,
        sku_recom.channel_name AS channel,
        NULL AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        sku_recom.franchise_name AS prod_hier_l3,
        sku_recom.brand_name AS prod_hier_l4,
        sku_recom.variant_name AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        sku_recom.mothersku_name AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        sku_recom.msl_target AS ms_flag,
        sku_recom.msl_hit AS hit_ms_flag,
        NULL AS "y/n_flag",
        'Y' AS priority_store_flag,
        NULL AS questiontext,
        NULL AS ques_desc,
        1 AS value,
        0 AS mkt_share,
        NULL AS rej_reason,
        NULL AS photo_url
    FROM (
        SELECT derived_table2.year_month,
            derived_table2.channel_name,
            derived_table2.customer_code,
            derived_table2.customer_name,
            derived_table2.retailer_category_name,
            derived_table2.program_name,
            derived_table2.retailer_class,
            derived_table2.urc AS retailer_cd,
            derived_table2.retailer_name,
            derived_table2.region_name,
            derived_table2.salesman_code,
            derived_table2.salesman_name,
            derived_table2.franchise_name,
            derived_table2.brand_name,
            derived_table2.product_category_name,
            derived_table2.variant_name,
            derived_table2.mothersku_name,
            derived_table2.msl_target,
            CASE 
                WHEN derived_table2.msl_target IS NOT NULL
                    THEN derived_table2.msl_hit
                ELSE NULL::INTEGER
                END AS msl_hit
        FROM (
            SELECT a.year_month,
                a.channel_name,
                a.customer_code,
                a.customer_name,
                a.retailer_category_name,
                a.retailer_class,
                a.urc,
                a.retailer_name,
                a.mothersku_name,
                a.region_name,
                a.salesman_code,
                a.salesman_name,
                a.franchise_name,
                a.brand_name,
                a.product_category_name,
                a.variant_name,
                CASE 
                    WHEN a.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT
                        THEN c.columnname || ' ('::CHARACTER VARYING::TEXT || c.program_name || ')'::CHARACTER VARYING::TEXT
                    ELSE c.columnname
                    END AS program_name,
                CASE 
                    WHEN sum(a.msl_target::NUMERIC::NUMERIC(18, 0)) > 0::NUMERIC::NUMERIC(18, 0)
                        THEN 1
                    ELSE NULL::INTEGER
                    END AS msl_target,
                CASE 
                    WHEN sum(a.msl_hit::NUMERIC::NUMERIC(18, 0)) > 0::NUMERIC::NUMERIC(18, 0)
                        THEN 1
                    ELSE 0
                    END AS msl_hit
            FROM (
                SELECT edw_sku_recom.mth_mm AS year_month,
                    edw_sku_recom.qtr,
                    edw_sku_recom.channel_name,
                    edw_sku_recom.retailer_category_name,
                    edw_sku_recom.region_name,
                    edw_sku_recom.salesman_code,
                    edw_sku_recom.salesman_name,
                    edw_sku_recom.cust_cd AS customer_code,
                    edw_sku_recom.class_desc AS retailer_class,
                    edw_sku_recom.retailer_cd,
                    edw_sku_recom.rtruniquecode AS urc,
                    edw_sku_recom.franchise_name,
                    edw_sku_recom.brand_name,
                    edw_sku_recom.product_category_name,
                    edw_sku_recom.variant_name,
                    edw_sku_recom.mothersku_name,
                    edw_sku_recom.ms_flag AS msl_target,
                    edw_sku_recom.hit_ms_flag AS msl_hit,
                    to_date((edw_sku_recom.mth_mm || 15)::CHARACTER VARYING::TEXT, 'YYYYMMDD'::CHARACTER VARYING::TEXT) AS DATE,
                    "max" (edw_sku_recom.customer_name::TEXT) AS customer_name,
                    "max" (edw_sku_recom.retailer_name::TEXT) AS retailer_name
                FROM edw_sku_recom_spike_msl AS edw_sku_recom
                WHERE edw_sku_recom.mothersku_name IS NOT NULL
                    AND (
                        edw_sku_recom.channel_name::TEXT = 'GT'::CHARACTER VARYING::TEXT
                        OR edw_sku_recom.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT
                        )
                    AND "substring" (
                        edw_sku_recom.mth_mm::CHARACTER VARYING::TEXT,
                        0,
                        5
                        ) >= (
                        "date_part" (
                            YEAR,
                            convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)
                            ) - 1
                        )::CHARACTER VARYING::TEXT
                    AND edw_sku_recom.ms_flag::TEXT = 1::CHARACTER VARYING::TEXT
                GROUP BY edw_sku_recom.mth_mm,
                    edw_sku_recom.qtr,
                    to_date((edw_sku_recom.mth_mm || 15)::CHARACTER VARYING::TEXT, 'YYYYMMDD'::CHARACTER VARYING::TEXT),
                    edw_sku_recom.channel_name,
                    edw_sku_recom.retailer_category_name,
                    edw_sku_recom.region_name,
                    edw_sku_recom.salesman_code,
                    edw_sku_recom.salesman_name,
                    edw_sku_recom.cust_cd,
                    edw_sku_recom.class_desc,
                    edw_sku_recom.retailer_cd,
                    edw_sku_recom.rtruniquecode,
                    edw_sku_recom.franchise_name,
                    edw_sku_recom.brand_name,
                    edw_sku_recom.product_category_name,
                    edw_sku_recom.variant_name,
                    edw_sku_recom.mothersku_name,
                    edw_sku_recom.ms_flag,
                    edw_sku_recom.hit_ms_flag
                ) a
            INNER JOIN (
                SELECT derived_table1."year",
                    derived_table1.quarter,
                    derived_table1."month",
                    derived_table1.distcode,
                    derived_table1.retailer_code,
                    min(derived_table1.columnname::TEXT) AS columnname,
                    "max" (derived_table1.program_name::TEXT) AS program_name
                FROM (
                    SELECT t.cal_yr AS "year",
                        t.cal_qtr_1 AS quarter,
                        t.cal_mo_2 AS "month",
                        u.columnname,
                        u.columnvalue AS program_name,
                        u.mastervaluecode AS retailer_code,
                        u.distcode
                    FROM itg_udcdetails AS u
                    INNER JOIN edw_calendar_dim AS t ON right(u.columnname::TEXT, 4) = t.cal_yr::CHARACTER VARYING::TEXT
                        AND "left" (
                            split_part(u.columnname::TEXT, ' Q'::CHARACTER VARYING::TEXT, 2),
                            1
                            ) = t.cal_qtr_1::CHARACTER VARYING::TEXT
                        AND (
                            u.columnname::TEXT LIKE '%SSS Program Q%'::CHARACTER VARYING::TEXT
                            OR u.columnname::TEXT LIKE '%Platinum Q%'::CHARACTER VARYING::TEXT
                            )
                    WHERE u.mastername::TEXT = 'Retailer Master'::CHARACTER VARYING::TEXT
                        AND u.columnvalue IS NOT NULL
                        AND u.columnvalue::TEXT <> 'No'::CHARACTER VARYING::TEXT
                    GROUP BY t.cal_yr,
                        t.cal_qtr_1,
                        t.cal_mo_2,
                        u.columnname,
                        u.columnvalue,
                        u.mastervaluecode,
                        u.distcode
                    ) derived_table1
                GROUP BY derived_table1."year",
                    derived_table1.quarter,
                    derived_table1."month",
                    derived_table1.distcode,
                    derived_table1.retailer_code
                ) c ON a.retailer_cd::TEXT = c.retailer_code::TEXT
                AND a.qtr = c.quarter
                AND ltrim("left" (
                        a.year_month::CHARACTER VARYING::TEXT,
                        4
                        ), '0'::CHARACTER VARYING::TEXT) = c."year"::CHARACTER VARYING::TEXT
                AND ltrim(right(a.year_month::CHARACTER VARYING::TEXT, 2), '0'::CHARACTER VARYING::TEXT) = c."month"::CHARACTER VARYING::TEXT
                AND c.distcode::TEXT = a.customer_code::TEXT
            GROUP BY a.year_month,
                a.channel_name,
                a.customer_code,
                a.customer_name,
                a.retailer_category_name,
                CASE 
                    WHEN a.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT
                        THEN c.columnname || ' ('::CHARACTER VARYING::TEXT || c.program_name || ')'::CHARACTER VARYING::TEXT
                    ELSE c.columnname
                    END,
                a.retailer_class,
                a.urc,
                a.retailer_name,
                a.mothersku_name,
                a.region_name,
                a.salesman_code,
                a.salesman_name,
                a.franchise_name,
                a.brand_name,
                a.product_category_name,
                a.variant_name
            ) derived_table2
        WHERE derived_table2.msl_target IS NOT NULL
        ) sku_recom
    LEFT JOIN (
        SELECT edw_vw_ps_weights.market,
            edw_vw_ps_weights.kpi,
            edw_vw_ps_weights.channel,
            edw_vw_ps_weights.retail_environment,
            edw_vw_ps_weights.weight
        FROM edw_vw_ps_weights
        WHERE trim(upper(edw_vw_ps_weights.kpi::TEXT)) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS wt ON trim(upper(wt.retail_environment::TEXT)) = trim(upper(sku_recom.retailer_category_name::TEXT))
        AND trim(upper(wt.channel::TEXT)) = trim(upper(sku_recom.channel_name::TEXT))    
    ),

ct12 as
(
SELECT 'Merchandising_Response' AS dataset,
        msl.store_code AS customerid,
        "max" (msl.isp_code::TEXT)::CHARACTER VARYING AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        CASE 
            WHEN sum(msl.quantity) > 0::NUMERIC::NUMERIC(18, 0)
                THEN 'true'::CHARACTER VARYING
            ELSE 'false'::CHARACTER VARYING
            END AS presence,
        NULL AS outofstock,
        'MSL Compliance' AS kpi,
        to_date("substring" (
                msl.yearmo::TEXT,
                3,
                5
                ) || '-'::CHARACTER VARYING::TEXT || "left" (
                msl.yearmo::TEXT,
                2
                ) || '-15'::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            msl.yearmo::TEXT,
            3,
            5
            )::CHARACTER VARYING AS fisc_yr,
        (
            "substring" (
                msl.yearmo::TEXT,
                3,
                5
                ) || "left" (
                msl.yearmo::TEXT,
                2
                )
            )::INTEGER AS fisc_per,
        "max" (msl.isp_name::TEXT)::CHARACTER VARYING AS firstname,
        '' AS lastname,
        (coalesce(msl.store_code, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(msl.store_name, 'NA'::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS customername,
        'India' AS country,
        msl.region AS STATE,
        msl.chain AS storereference,
        msl.format AS storetype,
        'MT' AS channel,
        msl.chain AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        pka.gcph_franchise AS prod_hier_l3,
        pka.gcph_brand AS prod_hier_l4,
        pka.gcph_subbrand AS prod_hier_l5,
        pka.gcph_variant AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        msl.product_name AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        NULL AS ms_flag,
        NULL AS hit_ms_flag,
        NULL AS "y/n_flag",
        CASE 
            WHEN upper(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag,
        NULL AS questiontext,
        NULL AS ques_desc,
        NULL AS value,
        NULL AS mkt_share,
        NULL AS rej_reason,
        NULL AS photo_url
    FROM (
        SELECT edw_vw_ps_weights.market,
            edw_vw_ps_weights.kpi,
            edw_vw_ps_weights.channel,
            edw_vw_ps_weights.retail_environment,
            edw_vw_ps_weights.weight
        FROM edw_vw_ps_weights
        WHERE upper(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS wt,
        itg_in_perfectstore_msl AS msl
    LEFT JOIN (
        SELECT edw_product_key_attributes.matl_num,
            edw_product_key_attributes.matl_desc,
            edw_product_key_attributes.crt_on,
            edw_product_key_attributes.ctry_nm,
            edw_product_key_attributes.matl_type_cd,
            edw_product_key_attributes.matl_type_desc,
            edw_product_key_attributes.mega_brnd_cd,
            edw_product_key_attributes.mega_brnd_desc,
            edw_product_key_attributes.brnd_cd,
            edw_product_key_attributes.brnd_desc,
            edw_product_key_attributes.varnt_desc,
            edw_product_key_attributes.base_prod_desc,
            edw_product_key_attributes.put_up_desc,
            edw_product_key_attributes.prodh1,
            edw_product_key_attributes.prodh1_txtmd,
            edw_product_key_attributes.prodh2,
            edw_product_key_attributes.prodh2_txtmd,
            edw_product_key_attributes.prodh3,
            edw_product_key_attributes.prodh3_txtmd,
            edw_product_key_attributes.prodh4,
            edw_product_key_attributes.prodh4_txtmd,
            edw_product_key_attributes.prodh5,
            edw_product_key_attributes.prodh5_txtmd,
            edw_product_key_attributes.prodh6,
            edw_product_key_attributes.prodh6_txtmd,
            edw_product_key_attributes.prod_hier_cd,
            edw_product_key_attributes.gcph_franchise,
            edw_product_key_attributes.gcph_brand,
            edw_product_key_attributes.gcph_subbrand,
            edw_product_key_attributes.gcph_variant,
            edw_product_key_attributes.gcph_needstate,
            edw_product_key_attributes.gcph_category,
            edw_product_key_attributes.gcph_subcategory,
            edw_product_key_attributes.gcph_segment,
            edw_product_key_attributes.gcph_subsegment,
            edw_product_key_attributes.ean_upc,
            edw_product_key_attributes.apac_variant,
            edw_product_key_attributes.data_type,
            edw_product_key_attributes.description,
            edw_product_key_attributes.base_unit,
            edw_product_key_attributes."region",
            edw_product_key_attributes.regional_brand,
            edw_product_key_attributes.regional_subbrand,
            edw_product_key_attributes.regional_megabrand,
            edw_product_key_attributes.regional_franchise,
            edw_product_key_attributes.regional_franchise_group,
            edw_product_key_attributes.pka_franchise,
            edw_product_key_attributes.pka_franchise_description,
            edw_product_key_attributes.pka_brand,
            edw_product_key_attributes.pka_brand_description,
            edw_product_key_attributes.pka_subbrand,
            edw_product_key_attributes.pka_subbranddesc,
            edw_product_key_attributes.pka_variant,
            edw_product_key_attributes.pka_variantdesc,
            edw_product_key_attributes.pka_subvariant,
            edw_product_key_attributes.pka_subvariantdesc,
            edw_product_key_attributes.pka_flavor,
            edw_product_key_attributes.pka_flavordesc,
            edw_product_key_attributes.pka_ingredient,
            edw_product_key_attributes.pka_ingredientdesc,
            edw_product_key_attributes.pka_application,
            edw_product_key_attributes.pka_applicationdesc,
            edw_product_key_attributes.pka_strength,
            edw_product_key_attributes.pka_strengthdesc,
            edw_product_key_attributes.pka_shape,
            edw_product_key_attributes.pka_shapedesc,
            edw_product_key_attributes.pka_spf,
            edw_product_key_attributes.pka_spfdesc,
            edw_product_key_attributes.pka_cover,
            edw_product_key_attributes.pka_coverdesc,
            edw_product_key_attributes.pka_form,
            edw_product_key_attributes.pka_formdesc,
            edw_product_key_attributes.pka_size,
            edw_product_key_attributes.pka_sizedesc,
            edw_product_key_attributes.pka_character,
            edw_product_key_attributes.pka_charaterdesc,
            edw_product_key_attributes.pka_package,
            edw_product_key_attributes.pka_packagedesc,
            edw_product_key_attributes.pka_attribute13,
            edw_product_key_attributes.pka_attribute13desc,
            edw_product_key_attributes.pka_attribute14,
            edw_product_key_attributes.pka_attribute14desc,
            edw_product_key_attributes.pka_skuidentification,
            edw_product_key_attributes.pka_skuiddesc,
            edw_product_key_attributes.pka_onetimerel,
            edw_product_key_attributes.pka_onetimereldesc,
            edw_product_key_attributes.pka_productkey,
            edw_product_key_attributes.pka_productdesc,
            edw_product_key_attributes.pka_rootcode,
            edw_product_key_attributes.pka_rootcodedes,
            edw_product_key_attributes.nts_flag,
            edw_product_key_attributes.lst_nts,
            edw_product_key_attributes.load_date
        FROM edw_product_key_attributes
        WHERE edw_product_key_attributes.ctry_nm::TEXT = 'India'::CHARACTER VARYING::TEXT
        ) AS pka ON ltrim(msl.product_code::TEXT, '0'::CHARACTER VARYING::TEXT) = ltrim(pka.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)
    WHERE trim(upper(wt.retail_environment::TEXT)) = trim(upper(msl.format::TEXT))
        AND upper(msl.msl::TEXT) = 'YES'::CHARACTER VARYING::TEXT
        AND "substring" (
            msl.yearmo::TEXT,
            3,
            5
            ) >= (date_part(YEAR, convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)) - 2::DOUBLE PRECISION)::CHARACTER VARYING::TEXT
    GROUP BY msl.store_code,
        msl.yearmo,
        msl.store_name,
        msl.region,
        msl.chain,
        msl.format,
        pka.gcph_franchise,
        pka.gcph_brand,
        pka.gcph_subbrand,
        pka.gcph_variant,
        msl.product_name,
        wt.weight::DOUBLE PRECISION,
        CASE 
            WHEN upper(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END
),
ct2
AS (
    SELECT 'Merchandising_Response' AS dataset,
        msl.store_code AS customerid,
        "max" (msl.isp_code::TEXT)::CHARACTER VARYING AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        'true' AS presence,
        CASE 
            WHEN msl.quantity > 0::NUMERIC::NUMERIC(18, 0)
                THEN ''::CHARACTER VARYING
            ELSE 'true'::CHARACTER VARYING
            END AS outofstock,
        'OOS Compliance' AS kpi,
        to_date(to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            "replace" (
                to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            4
            )::CHARACTER VARYING AS fisc_yr,
        "substring" (
            "replace" (
                to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            6
            )::INTEGER AS fisc_per,
        "max" (msl.isp_name::TEXT)::CHARACTER VARYING AS firstname,
        '' AS lastname,
        (coalesce(msl.store_code, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(msl.store_name, 'NA'::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS customername,
        'India' AS country,
        msl.region AS STATE,
        msl.chain AS storereference,
        msl.format AS storetype,
        'MT' AS channel,
        msl.chain AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        pka.gcph_franchise AS prod_hier_l3,
        pka.gcph_brand AS prod_hier_l4,
        pka.gcph_subbrand AS prod_hier_l5,
        pka.gcph_variant AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        msl.product_name AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        NULL AS ms_flag,
        NULL AS hit_ms_flag,
        NULL AS "y/n_flag",
        CASE 
            WHEN upper(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag,
        NULL AS questiontext,
        NULL AS ques_desc,
        NULL AS value,
        NULL AS mkt_share,
        NULL AS rej_reason,
        NULL AS photo_url
    FROM (
        SELECT edw_vw_ps_weights.market,
            edw_vw_ps_weights.kpi,
            edw_vw_ps_weights.channel,
            edw_vw_ps_weights.retail_environment,
            edw_vw_ps_weights.weight
        FROM edw_vw_ps_weights
        WHERE upper(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.kpi::TEXT) = 'OSA COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS wt,
        itg_in_perfectstore_msl AS msl
    LEFT JOIN (
        SELECT edw_product_key_attributes.matl_num,
            edw_product_key_attributes.matl_desc,
            edw_product_key_attributes.crt_on,
            edw_product_key_attributes.ctry_nm,
            edw_product_key_attributes.matl_type_cd,
            edw_product_key_attributes.matl_type_desc,
            edw_product_key_attributes.mega_brnd_cd,
            edw_product_key_attributes.mega_brnd_desc,
            edw_product_key_attributes.brnd_cd,
            edw_product_key_attributes.brnd_desc,
            edw_product_key_attributes.varnt_desc,
            edw_product_key_attributes.base_prod_desc,
            edw_product_key_attributes.put_up_desc,
            edw_product_key_attributes.prodh1,
            edw_product_key_attributes.prodh1_txtmd,
            edw_product_key_attributes.prodh2,
            edw_product_key_attributes.prodh2_txtmd,
            edw_product_key_attributes.prodh3,
            edw_product_key_attributes.prodh3_txtmd,
            edw_product_key_attributes.prodh4,
            edw_product_key_attributes.prodh4_txtmd,
            edw_product_key_attributes.prodh5,
            edw_product_key_attributes.prodh5_txtmd,
            edw_product_key_attributes.prodh6,
            edw_product_key_attributes.prodh6_txtmd,
            edw_product_key_attributes.prod_hier_cd,
            edw_product_key_attributes.gcph_franchise,
            edw_product_key_attributes.gcph_brand,
            edw_product_key_attributes.gcph_subbrand,
            edw_product_key_attributes.gcph_variant,
            edw_product_key_attributes.gcph_needstate,
            edw_product_key_attributes.gcph_category,
            edw_product_key_attributes.gcph_subcategory,
            edw_product_key_attributes.gcph_segment,
            edw_product_key_attributes.gcph_subsegment,
            edw_product_key_attributes.ean_upc,
            edw_product_key_attributes.apac_variant,
            edw_product_key_attributes.data_type,
            edw_product_key_attributes.description,
            edw_product_key_attributes.base_unit,
            edw_product_key_attributes."region",
            edw_product_key_attributes.regional_brand,
            edw_product_key_attributes.regional_subbrand,
            edw_product_key_attributes.regional_megabrand,
            edw_product_key_attributes.regional_franchise,
            edw_product_key_attributes.regional_franchise_group,
            edw_product_key_attributes.pka_franchise,
            edw_product_key_attributes.pka_franchise_description,
            edw_product_key_attributes.pka_brand,
            edw_product_key_attributes.pka_brand_description,
            edw_product_key_attributes.pka_subbrand,
            edw_product_key_attributes.pka_subbranddesc,
            edw_product_key_attributes.pka_variant,
            edw_product_key_attributes.pka_variantdesc,
            edw_product_key_attributes.pka_subvariant,
            edw_product_key_attributes.pka_subvariantdesc,
            edw_product_key_attributes.pka_flavor,
            edw_product_key_attributes.pka_flavordesc,
            edw_product_key_attributes.pka_ingredient,
            edw_product_key_attributes.pka_ingredientdesc,
            edw_product_key_attributes.pka_application,
            edw_product_key_attributes.pka_applicationdesc,
            edw_product_key_attributes.pka_strength,
            edw_product_key_attributes.pka_strengthdesc,
            edw_product_key_attributes.pka_shape,
            edw_product_key_attributes.pka_shapedesc,
            edw_product_key_attributes.pka_spf,
            edw_product_key_attributes.pka_spfdesc,
            edw_product_key_attributes.pka_cover,
            edw_product_key_attributes.pka_coverdesc,
            edw_product_key_attributes.pka_form,
            edw_product_key_attributes.pka_formdesc,
            edw_product_key_attributes.pka_size,
            edw_product_key_attributes.pka_sizedesc,
            edw_product_key_attributes.pka_character,
            edw_product_key_attributes.pka_charaterdesc,
            edw_product_key_attributes.pka_package,
            edw_product_key_attributes.pka_packagedesc,
            edw_product_key_attributes.pka_attribute13,
            edw_product_key_attributes.pka_attribute13desc,
            edw_product_key_attributes.pka_attribute14,
            edw_product_key_attributes.pka_attribute14desc,
            edw_product_key_attributes.pka_skuidentification,
            edw_product_key_attributes.pka_skuiddesc,
            edw_product_key_attributes.pka_onetimerel,
            edw_product_key_attributes.pka_onetimereldesc,
            edw_product_key_attributes.pka_productkey,
            edw_product_key_attributes.pka_productdesc,
            edw_product_key_attributes.pka_rootcode,
            edw_product_key_attributes.pka_rootcodedes,
            edw_product_key_attributes.nts_flag,
            edw_product_key_attributes.lst_nts,
            edw_product_key_attributes.load_date
        FROM edw_product_key_attributes
        WHERE edw_product_key_attributes.ctry_nm::TEXT = 'India'::CHARACTER VARYING::TEXT
        ) AS pka ON ltrim(msl.product_code::TEXT, '0'::CHARACTER VARYING::TEXT) = ltrim(pka.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)
    WHERE trim(upper(wt.retail_environment::TEXT)) = trim(upper(msl.format::TEXT))
        AND date_part(YEAR, to_date(to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE) >= (date_part(YEAR, convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)) - 2::DOUBLE PRECISION)
    GROUP BY msl.store_code,
        msl.quantity,
        msl.visit_datetime,
        msl.store_name,
        msl.region,
        msl.chain,
        msl.format,
        pka.gcph_franchise,
        pka.gcph_brand,
        pka.gcph_subbrand,
        pka.gcph_variant,
        msl.product_name,
        wt.weight::DOUBLE PRECISION,
        CASE 
            WHEN upper(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END
    ),
ct3
AS (
    SELECT 'Survey_Response' AS dataset,
        sos.store_code AS customerid,
        sos.isp_code AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        NULL AS presence,
        NULL AS outofstock,
        'Share of Shelf' AS kpi,
        to_date(to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            "replace" (
                to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            4
            )::CHARACTER VARYING AS fisc_yr,
        "substring" (
            "replace" (
                to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            6
            )::INTEGER AS fisc_per,
        sos.jnjisp_name AS firstname,
        '' AS lastname,
        (coalesce(sos.store_code, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(sos.store_name, 'NA'::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS customername,
        'India' AS country,
        sos.region AS STATE,
        sos.chain AS storereference,
        sos.format AS storetype,
        'MT' AS channel,
        sos.chain AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        sos.category AS prod_hier_l3,
        NULL AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        NULL AS ms_flag,
        NULL AS hit_ms_flag,
        NULL AS "y/n_flag",
        CASE 
            WHEN upper(sos.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag,
        NULL AS questiontext,
        'NUMERATOR' AS ques_desc,
        sos.prod_facings AS value,
        trgt.value AS mkt_share,
        NULL AS rej_reason,
        NULL AS photo_url
    FROM (
        SELECT edw_vw_ps_weights.market,
            edw_vw_ps_weights.kpi,
            edw_vw_ps_weights.channel,
            edw_vw_ps_weights.retail_environment,
            edw_vw_ps_weights.weight
        FROM edw_vw_ps_weights
        WHERE upper(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS wt,
        itg_in_perfectstore_sos AS sos
    LEFT JOIN (
        SELECT edw_vw_ps_targets.market,
            edw_vw_ps_targets.kpi,
            edw_vw_ps_targets.channel,
            edw_vw_ps_targets.retail_environment,
            edw_vw_ps_targets.attribute_1,
            edw_vw_ps_targets.attribute_2,
            edw_vw_ps_targets.value
        FROM edw_vw_ps_targets
        WHERE upper(edw_vw_ps_targets.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_targets.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_targets.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS trgt ON trgt.attribute_1::TEXT = sos.chain::TEXT
        AND trgt.attribute_2::TEXT = sos.category::TEXT
    WHERE trim(upper(wt.retail_environment::TEXT)) = trim(upper(sos.format::TEXT))
        AND date_part(YEAR, to_date(to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE) >= (date_part(YEAR, convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)) - 2::DOUBLE PRECISION)
    ),
ct4
AS (
    SELECT 'Survey_Response' AS dataset,
        sos.store_code AS customerid,
        sos.isp_code AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        NULL AS presence,
        NULL AS outofstock,
        'Share of Shelf' AS kpi,
        to_date(to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            "replace" (
                to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            4
            )::CHARACTER VARYING AS fisc_yr,
        "substring" (
            "replace" (
                to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            6
            )::INTEGER AS fisc_per,
        sos.jnjisp_name AS firstname,
        '' AS lastname,
        (coalesce(sos.store_code, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(sos.store_name, 'NA'::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS customername,
        'India' AS country,
        sos.region AS STATE,
        sos.chain AS storereference,
        sos.format AS storetype,
        'MT' AS channel,
        sos.chain AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        sos.category AS prod_hier_l3,
        NULL AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        NULL AS ms_flag,
        NULL AS hit_ms_flag,
        NULL AS "y/n_flag",
        NULL AS questiontext,
        'DENOMINATOR' AS ques_desc,
        sos.total_facings AS value,
        trgt.value AS mkt_share,
        NULL AS rej_reason,
        NULL AS photo_url,
        CASE 
            WHEN upper(sos.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag
    FROM (
        SELECT edw_vw_ps_weights.market,
            edw_vw_ps_weights.kpi,
            edw_vw_ps_weights.channel,
            edw_vw_ps_weights.retail_environment,
            edw_vw_ps_weights.weight
        FROM edw_vw_ps_weights
        WHERE upper(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS wt,
        itg_in_perfectstore_sos AS sos
    LEFT JOIN (
        SELECT edw_vw_ps_targets.market,
            edw_vw_ps_targets.kpi,
            edw_vw_ps_targets.channel,
            edw_vw_ps_targets.retail_environment,
            edw_vw_ps_targets.attribute_1,
            edw_vw_ps_targets.attribute_2,
            edw_vw_ps_targets.value
        FROM edw_vw_ps_targets
        WHERE upper(edw_vw_ps_targets.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_targets.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
            AND upper(edw_vw_ps_targets.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
        ) AS trgt ON trgt.attribute_1::TEXT = sos.chain::TEXT
        AND trgt.attribute_2::TEXT = sos.category::TEXT
    WHERE trim(upper(wt.retail_environment::TEXT)) = trim(upper(sos.format::TEXT))
        AND date_part(YEAR, to_date(to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE) >= (date_part(YEAR, convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)) - 2::DOUBLE PRECISION)
    ),
ct5
AS (
    SELECT 'Survey_Response' AS dataset,
        prmc.store_code AS customerid,
        prmc.isp_code AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        NULL AS presence,
        NULL AS outofstock,
        'Promo Compliance' AS kpi,
        to_date(to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            "replace" (
                to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            4
            )::CHARACTER VARYING AS fisc_yr,
        "substring" (
            "replace" (
                to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            6
            )::INTEGER AS fisc_per,
        prmc.isp_name AS firstname,
        '' AS lastname,
        (coalesce(prmc.store_code, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(prmc.store_name, 'NA'::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS customername,
        'India' AS country,
        prmc.region AS STATE,
        prmc.chain AS storereference,
        prmc.format AS storetype,
        'MT' AS channel,
        prmc.chain AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        prmc.product_category AS prod_hier_l3,
        prmc.product_brand AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        NULL AS ms_flag,
        NULL AS hit_ms_flag,
        CASE 
            WHEN upper(prmc.ispromotionavailable::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'YES'::CHARACTER VARYING
            ELSE 'NO'::CHARACTER VARYING
            END AS "y/n_flag",
        CASE 
            WHEN upper(prmc.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag,
        (('Is the '::CHARACTER VARYING::TEXT || coalesce(prmc.promotion_product_name, ''::CHARACTER VARYING)::TEXT || ' '::CHARACTER VARYING::TEXT || coalesce(prmc.promotionoffertype, ''::CHARACTER VARYING)::TEXT || ' promotion available?'::CHARACTER VARYING::TEXT))::CHARACTER VARYING(255) AS questiontext,
        NULL AS ques_desc,
        NULL AS value,
        NULL AS mkt_share,
        (coalesce(prmc.notavailablereason, ''::CHARACTER VARYING)::TEXT || ' '::CHARACTER VARYING::TEXT || coalesce(prmc.price_off, ''::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS rej_reason,
        prmc.photopath AS photo_url
    FROM (
        SELECT itg_in_perfectstore_promo.store_code,
            itg_in_perfectstore_promo.store_name,
            itg_in_perfectstore_promo.isp_code,
            itg_in_perfectstore_promo.isp_name,
            itg_in_perfectstore_promo.region,
            itg_in_perfectstore_promo.chain,
            itg_in_perfectstore_promo.format,
            itg_in_perfectstore_promo.product_category,
            itg_in_perfectstore_promo.product_brand,
            "max" (to_date(itg_in_perfectstore_promo.visit_datetime)) AS visit_datetime
        FROM itg_in_perfectstore_promo
        GROUP BY itg_in_perfectstore_promo.store_code,
            itg_in_perfectstore_promo.store_name,
            itg_in_perfectstore_promo.isp_code,
            itg_in_perfectstore_promo.isp_name,
            itg_in_perfectstore_promo.region,
            itg_in_perfectstore_promo.chain,
            itg_in_perfectstore_promo.format,
            itg_in_perfectstore_promo.product_category,
            itg_in_perfectstore_promo.product_brand
        ) AS prm,
        itg_in_perfectstore_promo AS prmc,
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE upper(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                AND upper(edw_vw_ps_weights.kpi::TEXT) = 'PROMO COMPLIANCE'::CHARACTER VARYING::TEXT
                AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
            ) AS wt
    WHERE trim(upper(wt.retail_environment::TEXT)) = trim(upper(prm.format::TEXT))
        AND to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::TIMESTAMP WITHOUT TIME ZONE = prmc.visit_datetime
        AND prm.store_code::TEXT = prmc.store_code::TEXT
        AND prm.store_name::TEXT = prmc.store_name::TEXT
        AND prm.isp_code::TEXT = prmc.isp_code::TEXT
        AND prm.isp_name::TEXT = prmc.isp_name::TEXT
        AND prm.region::TEXT = prmc.region::TEXT
        AND prm.chain::TEXT = prmc.chain::TEXT
        AND prm.format::TEXT = prmc.format::TEXT
        AND prm.product_category::TEXT = prmc.product_category::TEXT
        AND prm.product_brand::TEXT = prmc.product_brand::TEXT
        AND date_part(YEAR, to_date(to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE) >= (date_part(YEAR, convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)) - 2::DOUBLE PRECISION)
    ),
ct6
AS (
    SELECT 'Survey_Response' AS dataset,
        dsp.store_code AS customerid,
        dsp.isp_code AS salespersonid,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        NULL AS presence,
        NULL AS outofstock,
        'Display Compliance' AS kpi,
        to_date(to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT) AS scheduleddate,
        'completed' AS vst_status,
        "substring" (
            "replace" (
                to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            4
            )::CHARACTER VARYING AS fisc_yr,
        "substring" (
            "replace" (
                to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT,
                '-'::CHARACTER VARYING::TEXT,
                ''::CHARACTER VARYING::TEXT
                ),
            0,
            6
            )::INTEGER AS fisc_per,
        dsp.isp_name AS firstname,
        '' AS lastname,
        (coalesce(dsp.store_code, 'NA'::CHARACTER VARYING)::TEXT || '-'::CHARACTER VARYING::TEXT || coalesce(dsp.store_name, 'NA'::CHARACTER VARYING)::TEXT)::CHARACTER VARYING AS customername,
        'India' AS country,
        dsp.region AS STATE,
        dsp.chain AS storereference,
        dsp.format AS storetype,
        'MT' AS channel,
        dsp.chain AS salesgroup,
        'India' AS prod_hier_l1,
        NULL AS prod_hier_l2,
        dsp.product_category AS prod_hier_l3,
        dsp.product_brand AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        wt.weight::DOUBLE PRECISION AS kpi_chnl_wt,
        NULL AS ms_flag,
        NULL AS hit_ms_flag,
        CASE 
            WHEN upper(dsp.audit_status::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                AND upper(dsp.is_available::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'YES'::CHARACTER VARYING
            ELSE 'NO'::CHARACTER VARYING
            END AS "y/n_flag",
        CASE 
            WHEN upper(dsp.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                THEN 'Y'::CHARACTER VARYING
            ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag,
        (('Is the '::CHARACTER VARYING::TEXT || coalesce(dsp.posm_brand, ''::CHARACTER VARYING)::TEXT || ' available?'::CHARACTER VARYING::TEXT))::CHARACTER VARYING(255) AS questiontext,
        NULL AS ques_desc,
        NULL AS value,
        NULL AS mkt_share,
        dsp.reason AS rej_reason,
        NULL AS photo_url
    FROM itg_in_perfectstore_paid_display AS dsp,
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE upper(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                AND upper(edw_vw_ps_weights.kpi::TEXT) = 'DISPLAY COMPLIANCE'::CHARACTER VARYING::TEXT
                AND upper(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
            ) AS wt
    WHERE trim(upper(wt.retail_environment::TEXT)) = trim(upper(dsp.format::TEXT))
        AND date_part(YEAR, to_date(to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT, 'YYYY-MM-DD'::CHARACTER VARYING::TEXT)::TIMESTAMP WITHOUT TIME ZONE) >= (date_part(YEAR, convert_timezone('UTC', current_timestamp())::TIMESTAMP_NTZ(9)) - 2::DOUBLE PRECISION)
    ),
final
AS (
    SELECT *
    FROM ct1
    
    UNION ALL

    SELECt * from ct12

    union all
    SELECT *
    FROM ct2
        UNION ALL
        SELECT *
        FROM ct3
        UNION ALL
        SELECT *
        FROM ct4
        UNION ALL
        SELECT *
        FROM ct5
        UNION ALL
        SELECT *
        FROM ct6
    )
SELECT *
FROM final