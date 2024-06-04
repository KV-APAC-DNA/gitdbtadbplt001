with v_rpt_copa as
(
    select * from {{ ref('ntaedw_integration__v_rpt_copa') }}
),
edw_fert_material_fact as
(
    select * from {{ ref('ntaedw_integration__edw_fert_material_fact') }}
),
itg_mds_kr_cost_of_goods as
(
    select * from {{ ref('ntaitg_integration__itg_mds_kr_cost_of_goods') }} 
),
itg_kr_sales_store_map as
(
    select * from {{ ref('ntaitg_integration__itg_kr_sales_store_map') }} 
),
edw_gch_producthierarchy as
(
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }} 
),
edw_product_attr_dim as
(
    select * from aspedw_integration.edw_product_attr_dim
),
edw_product_key_attributes as
(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }} 
),
edw_billing_fact as
(
    select * from {{ ref('aspedw_integration__edw_billing_fact') }}
),
edw_intrm_calendar as
(
    select * from {{ ref('ntaedw_integration__edw_intrm_calendar') }} 
),
edw_material_sales_dim as
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
itg_kr_sales_tgt as
(
    select * from {{ ref('ntaitg_integration__itg_kr_sales_tgt') }}
),
v_intrm_reg_crncy_exch_fiscper as
(
    select * from {{ ref('aspedw_integration__v_intrm_reg_crncy_exch_fiscper') }}
),
itg_kr_tp_tracker_target as
(
    select * from {{ ref('ntaitg_integration__itg_kr_tp_tracker_target') }}
),
prod_hier as 
(
    SELECT derived_table1.prod_hier_l4,
        derived_table1.gcph_franchise
    FROM 
        (
            SELECT 
                DISTINCT attr.prod_hier_l4,
                CASE
                    WHEN ((attr.prod_hier_l4)::text = 'Aveeno Baby'::text) THEN 'Essential Health'::character varying
                    WHEN ((attr.prod_hier_l4)::text = 'Aveeno H&B'::text) THEN 'Skin Health'::character varying
                    WHEN ((attr.prod_hier_l4)::text = 'Listerine'::text) THEN 'Essential Health'::character varying
                    ELSE a.gcph_franchise
                END AS gcph_franchise
            FROM 
                (
                    edw_product_attr_dim attr
                    LEFT JOIN 
                    (
                        SELECT 
                            a.ctry_nm,
                            a.ean_upc,
                            a.gcph_franchise,
                            a.pka_franchise_description,
                            a.gcph_category,
                            a.gcph_subcategory,
                            a.pka_brand_description,
                            a.pka_subbranddesc,
                            a.pka_variantdesc,
                            a.pka_subvariantdesc,
                            a.pka_package,
                            a.pka_rootcode,
                            a.pka_productdesc,
                            a.pka_sizedesc,
                            a.pka_skuiddesc
                        FROM 
                            (
                                (
                                    SELECT edw_product_key_attributes.ctry_nm,
                                        edw_product_key_attributes.gcph_franchise,
                                        edw_product_key_attributes.pka_franchise_description,
                                        edw_product_key_attributes.gcph_category,
                                        edw_product_key_attributes.gcph_subcategory,
                                        edw_product_key_attributes.pka_brand_description,
                                        edw_product_key_attributes.pka_subbranddesc,
                                        edw_product_key_attributes.pka_variantdesc,
                                        edw_product_key_attributes.pka_subvariantdesc,
                                        edw_product_key_attributes.pka_package,
                                        edw_product_key_attributes.pka_rootcode,
                                        edw_product_key_attributes.pka_productdesc,
                                        edw_product_key_attributes.pka_sizedesc,
                                        edw_product_key_attributes.pka_skuiddesc,
                                        ltrim(
                                            (edw_product_key_attributes.ean_upc)::text,
                                            '0'::text
                                        ) AS ean_upc,
                                        edw_product_key_attributes.lst_nts AS nts_date
                                    FROM edw_product_key_attributes
                                    WHERE 
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (edw_product_key_attributes.matl_type_cd)::text = 'FERT'::text
                                                        )
                                                        OR (
                                                            (edw_product_key_attributes.matl_type_cd)::text = 'HALB'::text
                                                        )
                                                    )
                                                    OR (
                                                        (edw_product_key_attributes.matl_type_cd)::text = 'SAPR'::text
                                                    )
                                                )
                                                AND (edw_product_key_attributes.lst_nts IS NOT NULL)
                                            )
                                            AND (
                                                (edw_product_key_attributes.ctry_nm)::text = 'Korea'::text
                                            )
                                        )
                                    GROUP BY edw_product_key_attributes.ctry_nm,
                                        edw_product_key_attributes.gcph_franchise,
                                        edw_product_key_attributes.pka_franchise_description,
                                        edw_product_key_attributes.gcph_category,
                                        edw_product_key_attributes.gcph_subcategory,
                                        edw_product_key_attributes.pka_brand_description,
                                        edw_product_key_attributes.pka_subbranddesc,
                                        edw_product_key_attributes.pka_variantdesc,
                                        edw_product_key_attributes.pka_subvariantdesc,
                                        edw_product_key_attributes.pka_package,
                                        edw_product_key_attributes.pka_rootcode,
                                        edw_product_key_attributes.pka_productdesc,
                                        edw_product_key_attributes.pka_sizedesc,
                                        edw_product_key_attributes.pka_skuiddesc,
                                        ltrim(
                                            (edw_product_key_attributes.ean_upc)::text,
                                            '0'::text
                                        ),
                                        edw_product_key_attributes.lst_nts
                                ) a
                                JOIN 
                                (
                                    SELECT edw_product_key_attributes.ctry_nm,
                                        ltrim(
                                            (edw_product_key_attributes.ean_upc)::text,
                                            '0'::text
                                        ) AS ean_upc,
                                        edw_product_key_attributes.pka_rootcode,
                                        edw_product_key_attributes.lst_nts AS latest_nts_date,
                                        row_number() OVER(
                                            PARTITION BY edw_product_key_attributes.ctry_nm,
                                            edw_product_key_attributes.ean_upc
                                            ORDER BY edw_product_key_attributes.lst_nts DESC
                                        ) AS row_number
                                    FROM edw_product_key_attributes
                                    WHERE 
                                        (
                                            (
                                                (
                                                    (
                                                        (edw_product_key_attributes.matl_type_cd)::text = 'FERT'::text
                                                    )
                                                    OR (
                                                        (edw_product_key_attributes.matl_type_cd)::text = 'HALB'::text
                                                    )
                                                )
                                                OR (
                                                    (edw_product_key_attributes.matl_type_cd)::text = 'SAPR'::text
                                                )
                                            )
                                            AND (edw_product_key_attributes.lst_nts IS NOT NULL)
                                        )
                                    GROUP BY edw_product_key_attributes.ctry_nm,
                                        edw_product_key_attributes.ean_upc,
                                        edw_product_key_attributes.pka_rootcode,
                                        edw_product_key_attributes.lst_nts
                                ) b ON 
                                (
                                    (
                                        (
                                            (
                                                (
                                                    ((a.ctry_nm)::text = (b.ctry_nm)::text)
                                                    AND (a.ean_upc = b.ean_upc)
                                                )
                                                AND ((a.pka_rootcode)::text = (b.pka_rootcode)::text)
                                            )
                                            AND (b.latest_nts_date = a.nts_date)
                                        )
                                        AND (b.row_number = 1)
                                    )
                                )
                            )
                        GROUP BY a.ctry_nm,
                            a.ean_upc,
                            a.gcph_franchise,
                            a.pka_franchise_description,
                            a.gcph_category,
                            a.gcph_subcategory,
                            a.pka_brand_description,
                            a.pka_subbranddesc,
                            a.pka_variantdesc,
                            a.pka_subvariantdesc,
                            a.pka_package,
                            a.pka_rootcode,
                            a.pka_productdesc,
                            a.pka_sizedesc,
                            a.pka_skuiddesc
                    ) a ON 
                    (
                        (
                            (
                                (
                                    ltrim((attr.ean)::text, (0)::text) = ltrim(a.ean_upc, (0)::text)
                                )
                                AND ((attr.cntry)::text = 'KR'::text)
                            )
                            AND (
                                (attr.awrefs_buss_unit)::text = (a.ctry_nm)::text
                            )
                        )
                    )
                )
        ) derived_table1
    WHERE (derived_table1.gcph_franchise IS NOT NULL)
),
exrt as
(
    SELECT a.fisc_yr_per AS fisc_per,
        COALESCE(a.ex_rt_typ, b.ex_rt_typ) AS ex_rt_typ,
        COALESCE(a.from_crncy, b.from_crncy) AS from_crncy,
        COALESCE(a.to_crncy, b.to_crncy) AS to_crncy,
        COALESCE(a.ex_rt, b.ex_rt) AS ex_rt
    FROM 
        (
            (
                SELECT DISTINCT tgt.fisc_yr_per,
                    ex_rt.ex_rt_typ,
                    ex_rt.from_crncy,
                    ex_rt.to_crncy,
                    ex_rt.ex_rt
                FROM 
                    (
                        itg_kr_sales_tgt tgt
                        LEFT JOIN 
                        (
                            SELECT DISTINCT v_intrm_reg_crncy_exch_fiscper.ex_rt_typ,
                                v_intrm_reg_crncy_exch_fiscper.from_crncy,
                                v_intrm_reg_crncy_exch_fiscper.fisc_per,
                                v_intrm_reg_crncy_exch_fiscper.to_crncy,
                                v_intrm_reg_crncy_exch_fiscper.ex_rt
                            FROM v_intrm_reg_crncy_exch_fiscper
                            WHERE 
                            (
                                    (
                                        upper(
                                            (v_intrm_reg_crncy_exch_fiscper.from_crncy)::text
                                        ) = ('KRW'::character varying)::text
                                    )
                                    AND (
                                        (
                                            (
                                                upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('USD'::character varying)::text
                                            )
                                            OR (
                                                upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('KRW'::character varying)::text
                                            )
                                        )
                                        OR (
                                            upper((v_intrm_reg_crncy_exch_fiscper.to_crncy)::text) = ('SGD'::character varying)::text
                                        )
                                    )
                                )
                        ) ex_rt ON ((tgt.fisc_yr_per = ex_rt.fisc_per))
                    )
            ) a
            LEFT JOIN 
            (
                SELECT v_intrm_reg_crncy_exch_fiscper.ex_rt_typ,
                    v_intrm_reg_crncy_exch_fiscper.from_crncy,
                    v_intrm_reg_crncy_exch_fiscper.to_crncy,
                    v_intrm_reg_crncy_exch_fiscper.ex_rt
                FROM v_intrm_reg_crncy_exch_fiscper
                WHERE 
                    (
                        (
                            (
                                (v_intrm_reg_crncy_exch_fiscper.from_crncy)::text = 'KRW'::text
                            )
                            AND (
                                (
                                    (
                                        (v_intrm_reg_crncy_exch_fiscper.to_crncy)::text = 'KRW'::text
                                    )
                                    OR (
                                        (v_intrm_reg_crncy_exch_fiscper.to_crncy)::text = 'SGD'::text
                                    )
                                )
                                OR (
                                    (v_intrm_reg_crncy_exch_fiscper.to_crncy)::text = 'USD'::text
                                )
                            )
                        )
                        AND (
                            v_intrm_reg_crncy_exch_fiscper.fisc_per IN (
                                SELECT "max"(v_intrm_reg_crncy_exch_fiscper.fisc_per) AS "max"
                                FROM v_intrm_reg_crncy_exch_fiscper
                                WHERE (
                                        (
                                            (v_intrm_reg_crncy_exch_fiscper.from_crncy)::text = 'KRW'::text
                                        )
                                        AND (
                                            (
                                                (
                                                    (v_intrm_reg_crncy_exch_fiscper.to_crncy)::text = 'KRW'::text
                                                )
                                                OR (
                                                    (v_intrm_reg_crncy_exch_fiscper.to_crncy)::text = 'SGD'::text
                                                )
                                            )
                                            OR (
                                                (v_intrm_reg_crncy_exch_fiscper.to_crncy)::text = 'USD'::text
                                            )
                                        )
                                    )
                            )
                        )
                    )
            ) b ON ((a.ex_rt IS NULL))
        )
),
a as
(
    SELECT 
        v_rpt_copa.cntry_key,
        v_rpt_copa.cust_num,
        v_rpt_copa.channel,
        v_rpt_copa.prft_ctr,
        v_rpt_copa.matl,
        v_rpt_copa.matl_desc,
        v_rpt_copa.mega_brnd_desc,
        v_rpt_copa.brnd_desc,
        v_rpt_copa.prod_minor,
        v_rpt_copa.sls_grp,
        v_rpt_copa.sls_grp_desc,
        v_rpt_copa.sls_ofc,
        v_rpt_copa.sls_ofc_desc,
        v_rpt_copa.category_1,
        v_rpt_copa.categroy_2,
        v_rpt_copa.platform_ca,
        sum(v_rpt_copa.amt_obj_crncy) AS amt_obj_crncy,
        v_rpt_copa.obj_crncy_co_obj,
        v_rpt_copa.edw_cust_nm,
        v_rpt_copa.from_crncy,
        v_rpt_copa.to_crncy,
        v_rpt_copa.ex_rt_typ,
        v_rpt_copa.ex_rt,
        v_rpt_copa.brand_group,
        v_rpt_copa.fisc_day,
        v_rpt_copa.fisc_yr_per,
        v_rpt_copa.acct_hier_desc,
        v_rpt_copa.acct_hier_shrt_desc,
        v_rpt_copa.cntry_cd,
        v_rpt_copa.company_nm,
        v_rpt_copa.store_type,
        v_rpt_copa.plnt
    FROM v_rpt_copa v_rpt_copa
    WHERE 
        (
            (
                (
                    (
                        (v_rpt_copa.acct_hier_shrt_desc)::text = 'GTS'::text
                    )
                    OR (
                        (v_rpt_copa.acct_hier_shrt_desc)::text = 'NTS'::text
                    )
                )
                OR (
                    (v_rpt_copa.acct_hier_shrt_desc)::text = 'FG'::text
                )
            )
            AND (
                date_part(
                    year,
                    (v_rpt_copa.fisc_day)::timestamp without time zone
                ) >= (
                    date_part(
                        year,
                        convert_timezone('UTC', current_timestamp())::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
    GROUP BY v_rpt_copa.cntry_key,
        v_rpt_copa.cust_num,
        v_rpt_copa.channel,
        v_rpt_copa.prft_ctr,
        v_rpt_copa.matl,
        v_rpt_copa.matl_desc,
        v_rpt_copa.mega_brnd_desc,
        v_rpt_copa.brnd_desc,
        v_rpt_copa.prod_minor,
        v_rpt_copa.sls_grp,
        v_rpt_copa.sls_grp_desc,
        v_rpt_copa.sls_ofc,
        v_rpt_copa.sls_ofc_desc,
        v_rpt_copa.category_1,
        v_rpt_copa.categroy_2,
        v_rpt_copa.platform_ca,
        v_rpt_copa.obj_crncy_co_obj,
        v_rpt_copa.edw_cust_nm,
        v_rpt_copa.from_crncy,
        v_rpt_copa.to_crncy,
        v_rpt_copa.ex_rt_typ,
        v_rpt_copa.ex_rt,
        v_rpt_copa.brand_group,
        v_rpt_copa.fisc_day,
        v_rpt_copa.fisc_yr_per,
        v_rpt_copa.acct_hier_desc,
        v_rpt_copa.acct_hier_shrt_desc,
        v_rpt_copa.cntry_cd,
        v_rpt_copa.company_nm,
        v_rpt_copa.store_type,
        v_rpt_copa.plnt
),
sales_1 as 
(   
    SELECT 
        'Sales' AS identifier,
        a.cntry_key,
        (
            COALESCE(ltrim((a.cust_num)::text, (0)::text), 'NA'::text)
        )::character varying AS cust_num,
        COALESCE(store_map.channel, a.channel) AS channel,
        a.prft_ctr,
        (a.matl)::character varying AS matl,
        a.matl_desc,
        a.mega_brnd_desc,
        CASE
            WHEN ((a.brnd_desc)::text = 'Not Available'::text) THEN 'NA'::character varying
            ELSE a.brnd_desc
        END AS brnd_desc,
        COALESCE(
            CASE
                WHEN ((a.brnd_desc)::text = 'Aveeno Baby'::text) THEN 'Essential Health'::character varying
                WHEN ((a.brnd_desc)::text = 'Aveeno H&B'::text) THEN 'Skin Health'::character varying
                WHEN ((a.brnd_desc)::text = 'Listerine'::text) THEN 'Essential Health'::character varying
                WHEN (
                    ((a.brnd_desc)::text = 'NA'::text)
                    OR ((a.brnd_desc)::text = 'Not Available'::text)
                ) THEN 'NA'::character varying
                ELSE gcph_prod_hier.gcph_franchise
            END,
            prod_hier.gcph_franchise
        ) AS franchise,
        a.prod_minor,
        a.sls_grp,
        COALESCE(
            store_map.sales_group_nm,
            'NA'::character varying
        ) AS sls_grp_desc,
        COALESCE(
            store_map.sls_ofc,
            'Not Available'::character varying
        ) AS sls_ofc,
        COALESCE(
            store_map.sls_ofc_desc,
            'Not Available'::character varying
        ) AS sls_ofc_desc,
        a.category_1,
        a.categroy_2,
        a.platform_ca,
        a.amt_obj_crncy,
        a.obj_crncy_co_obj,
        COALESCE(
            a.edw_cust_nm,
            'Not Available'::character varying
        ) AS edw_cust_nm,
        a.from_crncy,
        a.to_crncy,
        a.ex_rt_typ,
        a.ex_rt,
        a.brand_group,
        a.fisc_day,
        a.fisc_yr_per,
        0 AS qty,
        0 AS units_sold,
        null AS uom,
        a.acct_hier_desc,
        a.acct_hier_shrt_desc,
        a.cntry_cd,
        a.company_nm,
        CASE
            WHEN (a.matl = (b.matl_num)::text) THEN 'Y'::character varying
            ELSE 'N'::character varying
        END AS fert_flag,
        COALESCE(store_map.store_type, 'NA'::character varying) AS store_type,
        cogs.free_good_value,
        cogs.pre_apsc_cogs,
        cogs.package_cost,
        cogs.labour_cost,
        (
            CASE
                WHEN ((a.matl_desc)::text like 'OP%'::text) THEN '8000'::text
                ELSE '7920'::text
            END
        )::character varying AS valuation_class,
        'NA' AS target_type,
        0 AS target_value,
        0 AS target_amount
    FROM a
        LEFT JOIN 
        (
            SELECT DISTINCT edw_fert_material_fact.matl_num
            FROM edw_fert_material_fact
        ) b ON ((a.matl = (b.matl_num)::text))
        LEFT JOIN itg_mds_kr_cost_of_goods cogs ON 
        (
            (
                (
                    (a.matl = (cogs.sap_code)::text)
                    AND (
                        (a.fisc_day >= cogs.valid_from)
                        AND (a.fisc_day <= cogs.valid_to)
                    )
                )
                AND ((a.cntry_cd)::text = 'KR'::text)
            )
        )
        LEFT JOIN itg_kr_sales_store_map store_map ON 
        (
            (store_map.sales_group_code)::text = (a.sls_grp)::text
        )           
        LEFT JOIN 
        (
            SELECT 
                edw_gch_producthierarchy.brnd_tamr_id,
                edw_gch_producthierarchy.ctgy_tamr_id,
                edw_gch_producthierarchy.brnd_origin_source_name,
                edw_gch_producthierarchy.ctgy_origin_source_name,
                edw_gch_producthierarchy.brnd_origin_entity_id,
                edw_gch_producthierarchy.ctgy_origin_entity_id,
                edw_gch_producthierarchy.brnd_unique_id,
                edw_gch_producthierarchy.ctgy_unique_id,
                edw_gch_producthierarchy.brnd_manualclassificationid,
                edw_gch_producthierarchy.ctgy_manualclassificationid,
                edw_gch_producthierarchy.brnd_manualclassificationpath,
                edw_gch_producthierarchy.ctgy_manualclassificationpath,
                edw_gch_producthierarchy.brnd_suggestedclassificationid,
                edw_gch_producthierarchy.ctgy_suggestedclassificationid,
                edw_gch_producthierarchy.brnd_suggestedclassificationpath,
                edw_gch_producthierarchy.ctgy_suggestedclassificationpath,
                edw_gch_producthierarchy.brnd_suggestedclassificationscore,
                edw_gch_producthierarchy.ctgy_suggestedclassificationscore,
                edw_gch_producthierarchy.brnd_finalclassificationpath,
                edw_gch_producthierarchy.ctgy_finalclassificationpath,
                edw_gch_producthierarchy.materialnumber,
                edw_gch_producthierarchy."region",
                edw_gch_producthierarchy.gcph_franchise,
                edw_gch_producthierarchy.gcph_brand,
                edw_gch_producthierarchy.gcph_subbrand,
                edw_gch_producthierarchy.gcph_variant,
                edw_gch_producthierarchy.gcph_needstate,
                edw_gch_producthierarchy.gcph_category,
                edw_gch_producthierarchy.gcph_subcategory,
                edw_gch_producthierarchy.gcph_segment,
                edw_gch_producthierarchy.gcph_subsegment,
                edw_gch_producthierarchy.ean_upc,
                edw_gch_producthierarchy.emea_gbpbgc,
                edw_gch_producthierarchy.emea_gbpmgrc,
                edw_gch_producthierarchy.emea_prodh3,
                edw_gch_producthierarchy.apac_variant,
                edw_gch_producthierarchy.industry_sector,
                edw_gch_producthierarchy.market,
                edw_gch_producthierarchy.data_type,
                edw_gch_producthierarchy.family,
                edw_gch_producthierarchy.product,
                edw_gch_producthierarchy.product_hierarchy,
                edw_gch_producthierarchy.description,
                edw_gch_producthierarchy.division,
                edw_gch_producthierarchy.base_unit,
                edw_gch_producthierarchy.regional_brand,
                edw_gch_producthierarchy.regional_subbrand,
                edw_gch_producthierarchy.regional_megabrand,
                edw_gch_producthierarchy.regional_franchise,
                edw_gch_producthierarchy.regional_franchise_group,
                edw_gch_producthierarchy.material_group,
                edw_gch_producthierarchy.material_type,
                edw_gch_producthierarchy.unit,
                edw_gch_producthierarchy.order_unit,
                edw_gch_producthierarchy.size_dimension,
                edw_gch_producthierarchy.height,
                edw_gch_producthierarchy.length,
                edw_gch_producthierarchy.width,
                edw_gch_producthierarchy.volume,
                edw_gch_producthierarchy.volume_unit,
                edw_gch_producthierarchy.gross_weight,
                edw_gch_producthierarchy.net_weight,
                edw_gch_producthierarchy.weight_unit,
                edw_gch_producthierarchy.put_up_code,
                edw_gch_producthierarchy.put_up_description,
                edw_gch_producthierarchy.size,
                edw_gch_producthierarchy.unit_of_measure,
                edw_gch_producthierarchy.brnd_dateofextract,
                edw_gch_producthierarchy.ctgy_dateofextract,
                edw_gch_producthierarchy.brnd_cdl_datetime,
                edw_gch_producthierarchy.ctgy_cdl_datetime,
                edw_gch_producthierarchy.brnd_cdl_source_file,
                edw_gch_producthierarchy.ctgy_cdl_source_file,
                edw_gch_producthierarchy.brnd_load_key,
                edw_gch_producthierarchy.ctgy_load_key,
                edw_gch_producthierarchy.crt_dttm,
                edw_gch_producthierarchy.updt_dttm
            FROM edw_gch_producthierarchy
            WHERE 
                (
                    ltrim(
                        (edw_gch_producthierarchy.materialnumber)::text,
                        (0)::text
                    ) <> ''::text
                )
        ) gcph_prod_hier ON 
        (
            (
                ltrim(a.matl, (0)::text) = ltrim((gcph_prod_hier.materialnumber)::text, (0)::text)
            )
        )
        LEFT JOIN prod_hier ON 
        (
            (
                (prod_hier.prod_hier_l4)::text = (a.brnd_desc)::text
            )
        )
),
sales_south_korea as
(
    SELECT 
        'Sales' AS identifier,
        'South Korea' AS cntry_key,
        (
            COALESCE(
                ltrim((bill_fact.customer)::text, (0)::text),
                'NA'::text
            )
        )::character varying AS cust_num,
        store_map.channel,
        'NA' AS prft_ctr,
        (ltrim((bill_fact.material)::text, (0)::text))::character varying AS matl,
        matl_sales.med_desc AS matl_desc,
        COALESCE(prod_attr.prod_hier_l4, 'NA'::character varying) AS mega_brnd_desc,
        COALESCE(prod_attr.prod_hier_l4, 'NA'::character varying) AS brnd_desc,
        COALESCE(
            prod_hier.gcph_franchise,
            'NA'::character varying
        ) AS franchise,
        'NA' AS prod_minor,
        bill_fact.sls_grp,
        COALESCE(
            store_map.sales_group_nm,
            'NA'::character varying
        ) AS sls_grp_desc,
        COALESCE(store_map.sls_ofc, 'NA'::character varying) AS sls_ofc,
        COALESCE(store_map.sls_ofc_desc, 'NA'::character varying) AS sls_ofc_desc,
        'NA' AS category_1,
        'NA' AS categroy_2,
        'NA' AS platform_ca,
        0 AS amt_obj_crncy,
        'KRW' AS obj_crncy_co_obj,
        COALESCE(cust.cust_nm, 'Not Available'::character varying) AS edw_cust_nm,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.ex_rt_typ,
        exrt.ex_rt,
        'NA' AS brand_group,
        to_date(
            (
                (
                    "substring"((bill_fact.fisc_per)::text, 6, 8) || '01'::text
                ) || "substring"((bill_fact.fisc_per)::text, 1, 4)
            ),
            'MMDDYYYY'::text
        ) AS fisc_day,
        bill_fact.fisc_per AS fisc_yr_per,
        bill_fact.qty,
        bill_fact.units_sold,
        (bill_fact.uom)::character varying AS uom,
        'Free Goods' AS acct_hier_desc,
        'FG' AS acct_hier_shrt_desc,
        'KR' AS cntry_cd,
        'NA' AS company_nm,
        (
            CASE
                WHEN (
                    ltrim((bill_fact.material)::text, (0)::text) = ltrim((fert_matl.matl_num)::text, (0)::text)
                ) THEN 'Y'::text
                ELSE 'N'::text
            END
        )::character varying AS fert_flag,
        COALESCE(store_map.store_type, 'NA'::character varying) AS store_type,
        0 AS free_good_value,
        0 AS pre_apsc_cogs,
        0 AS package_cost,
        0 AS labour_cost,
        (
            CASE
                WHEN ((matl_sales.med_desc)::text like 'OP%'::text) THEN '8000'::text
                ELSE '7920'::text
            END
        )::character varying AS valuation_class,
        'NA' AS target_type,
        0 AS target_value,
        0 AS target_amount
    FROM 
        (           
            (
                SELECT 
                    derived_table2.fisc_per,
                    edw_billing_fact.sls_grp,
                    edw_billing_fact.material,
                    edw_billing_fact.customer,
                    edw_billing_fact.plant,
                    edw_billing_fact.sls_org,
                    edw_billing_fact.distr_chnl,
                    sum(edw_billing_fact.bill_qty) AS qty,
                    sum(edw_billing_fact.inv_qty) AS units_sold,
                    (
                        ((edw_billing_fact.sls_unit)::text || ' - '::text) || split_part(
                            (
                                (
                                    edw_billing_fact.bill_qty / edw_billing_fact.inv_qty
                                )
                            )::text,
                            '.'::text,
                            1
                        )
                    ) AS uom
                FROM 
                    (
                        edw_billing_fact
                        LEFT JOIN (
                            SELECT DISTINCT edw_intrm_calendar.cal_day,
                                edw_intrm_calendar.cal_mo_1,
                                edw_intrm_calendar.fisc_yr,
                                edw_intrm_calendar.fisc_per,
                                edw_intrm_calendar.pstng_per
                            FROM edw_intrm_calendar
                        ) derived_table2 ON (
                            (
                                edw_billing_fact.bill_dt = derived_table2.cal_day
                            )
                        )
                    )
                WHERE 
                    (
                        (
                            (
                                (edw_billing_fact.bill_type)::text = 'ZFGD'::text
                            )
                            AND (
                                (edw_billing_fact.doc_currcy)::text = 'KRW'::text
                            )
                        )
                        AND (
                            date_part(
                                year,
                                (edw_billing_fact.bill_dt)::timestamp without time zone
                            ) >= (
                                date_part(
                                    year,
                                    convert_timezone('UTC', current_timestamp())::timestamp without time zone
                                ) - (2)::double precision
                            )
                        )
                    )
                GROUP BY derived_table2.fisc_per,
                    edw_billing_fact.sls_grp,
                    edw_billing_fact.material,
                    edw_billing_fact.customer,
                    edw_billing_fact.plant,
                    edw_billing_fact.sls_org,
                    edw_billing_fact.distr_chnl,
                    (
                        ((edw_billing_fact.sls_unit)::text || ' - '::text) || split_part(
                            (
                                (
                                    edw_billing_fact.bill_qty / edw_billing_fact.inv_qty
                                )
                            )::text,
                            '.'::text,
                            1
                        )
                    )
            ) bill_fact
            LEFT JOIN itg_kr_sales_store_map store_map ON 
            (
                (
                    (store_map.sales_group_code)::text = (bill_fact.sls_grp)::text
                )
            )
            LEFT JOIN 
            (
                SELECT DISTINCT edw_material_sales_dim.sls_org,
                    edw_material_sales_dim.dstr_chnl,
                    edw_material_sales_dim.matl_num,
                    edw_material_sales_dim.ean_num,
                    edw_material_sales_dim.med_desc
                FROM edw_material_sales_dim
                WHERE 
                    (
                        (
                            (edw_material_sales_dim.sls_org)::text = '320S'::text
                        )
                        AND (
                            (
                                (edw_material_sales_dim.ean_num)::text <> ''::text
                            )
                            OR (edw_material_sales_dim.ean_num IS NOT NULL)
                        )
                    )
            ) matl_sales ON 
            (
                (
                    (
                        (
                            (matl_sales.sls_org)::text = (bill_fact.sls_org)::text
                        )
                        AND (
                            (matl_sales.dstr_chnl)::text = (bill_fact.distr_chnl)::text
                        )
                    )
                    AND (
                        ltrim((matl_sales.matl_num)::text, (0)::text) = ltrim((bill_fact.material)::text, (0)::text)
                    )
                )
            )
            LEFT JOIN edw_product_attr_dim prod_attr ON 
            (
                (
                    (
                        ltrim((matl_sales.ean_num)::text, '0'::text) = ltrim((prod_attr.aw_remote_key)::text, '0'::text)
                    )
                    AND ((prod_attr.cntry)::text = 'KR'::text)
                )
            )
            LEFT JOIN prod_hier ON 
            (
                (
                    (prod_hier.prod_hier_l4)::text = (prod_attr.prod_hier_l4)::text
                )
            )
            LEFT JOIN exrt ON 
            (
                (
                    ((exrt.from_crncy)::text = 'KRW'::text)
                    AND (bill_fact.fisc_per = exrt.fisc_per)
                )
            ) 
            LEFT JOIN 
            (
                SELECT DISTINCT edw_fert_material_fact.matl_num FROM edw_fert_material_fact
            ) fert_matl ON 
            (
                (
                    ltrim((bill_fact.material)::text, (0)::text) = ltrim((fert_matl.matl_num)::text, (0)::text)
                )
            )  
            LEFT JOIN edw_customer_base_dim cust ON 
            (
                (
                    ltrim((bill_fact.customer)::text, (0)::text) = ltrim((cust.cust_num)::text)
                )
            )
        )
),
fg_target as 
(
    SELECT 
        'FG_TARGET' AS identifier,
        tgt.country_name AS cntry_key,
        'NA' AS cust_num,
        tgt.channel,
        'NA' AS prft_ctr,
        'NA' AS matl,
        'NA' AS matl_desc,
        'NA' AS mega_brnd_desc,
        tgt.brand AS brnd_desc,
        COALESCE(
            prod_hier.gcph_franchise,
            'NA'::character varying
        ) AS franchise,
        'NA' AS prod_minor,
        COALESCE(tgt.sales_group_cd, 'NA'::character varying) AS sls_grp,
        COALESCE(tgt.sales_group_name, 'NA'::character varying) AS sls_grp_desc,
        COALESCE(cust.sls_ofc, 'NA'::character varying) AS sls_ofc,
        COALESCE(cust.sls_ofc_desc, 'NA'::character varying) AS sls_ofc_desc,
        'NA' AS category_1,
        'NA' AS categroy_2,
        'NA' AS platform_ca,
        0 AS amt_obj_crncy,
        tgt.crncy_cd AS obj_crncy_co_obj,
        'Not Available' AS edw_cust_nm,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.ex_rt_typ,
        exrt.ex_rt,
        'NA' AS brand_group,
        tgt.tgt_date AS fisc_day,
        cal.fisc_per AS fisc_yr_per,
        0 AS qty,
        0 AS units_sold,
        'NA' AS uom,
        'NA' AS acct_hier_desc,
        'NA' AS acct_hier_shrt_desc,
        tgt.cntry_cd,
        'NA' AS company_nm,
        null AS fert_flag,
        COALESCE(tgt.store_type, 'NA'::character varying) AS store_type,
        0 AS free_good_value,
        0 AS pre_apsc_cogs,
        0 AS package_cost,
        0 AS labour_cost,
        null AS valuation_class,
        tgt.target_type,
        tgt.tgt_value AS target_value,
        tgt.target_amt AS target_amount
    FROM  itg_kr_tp_tracker_target tgt
        LEFT JOIN itg_kr_sales_store_map cust ON 
        (
            (
                COALESCE(
                    upper(
                        trim((tgt.sales_group_cd)::text,('NA'::character varying)::text)
                    )
                ,'NA') = COALESCE(
                    upper(
                        trim(
                            (cust.sales_group_code)::text,
                            ('NA'::character varying)::text
                        )
                    )
                ,'NA')
            )
        )
        LEFT JOIN 
        (
            SELECT DISTINCT edw_intrm_calendar.cal_day,
                edw_intrm_calendar.cal_mo_1,
                edw_intrm_calendar.fisc_yr,
                edw_intrm_calendar.fisc_per,
                edw_intrm_calendar.pstng_per
            FROM edw_intrm_calendar
        ) cal ON ((tgt.tgt_date = cal.cal_day))
        LEFT JOIN exrt ON 
        (
            (
                ((tgt.crncy_cd)::text = (exrt.from_crncy)::text)
                AND (cal.fisc_per = exrt.fisc_per)
            )
        )
        LEFT JOIN prod_hier ON 
        (
            (
                (prod_hier.prod_hier_l4)::text = (tgt.brand)::text
            )
        )        
    WHERE 
        (
            (
                upper((tgt.target_category_code)::text) = 'FG'::text
            )
            AND (
                (tgt.year)::double precision >= (
                    date_part(
                        year,
                        convert_timezone('UTC', current_timestamp())::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
),
fg_nts_target as 
(
    SELECT 
        (
            CASE
                WHEN ((tgt.target_category_code)::text = 'FG'::text) THEN 'FG_TARGET_YTD'::text
                WHEN ((tgt.target_category_code)::text = 'NTS'::text) THEN 'NTS_TARGET_YTD'::text
                ELSE 'FG_TARGET_YTD'::text
            END
        )::character varying AS identifier,
        tgt.country_name AS cntry_key,
        'NA' AS cust_num,
        tgt.channel,
        'NA' AS prft_ctr,
        'NA' AS matl,
        'NA' AS matl_desc,
        'NA' AS mega_brnd_desc,
        tgt.brand AS brnd_desc,
        COALESCE(
            prod_hier.gcph_franchise,
            'NA'::character varying
        ) AS franchise,
        'NA' AS prod_minor,
        COALESCE(tgt.sales_group_cd, 'NA'::character varying) AS sls_grp,
        COALESCE(tgt.sales_group_name, 'NA'::character varying) AS sls_grp_desc,
        COALESCE(cust.sls_ofc, 'NA'::character varying) AS sls_ofc,
        COALESCE(cust.sls_ofc_desc, 'NA'::character varying) AS sls_ofc_desc,
        'NA' AS category_1,
        'NA' AS categroy_2,
        'NA' AS platform_ca,
        0 AS amt_obj_crncy,
        tgt.crncy_cd AS obj_crncy_co_obj,
        'Not Available' AS edw_cust_nm,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.ex_rt_typ,
        exrt.ex_rt,
        'NA' AS brand_group,
        tgt.tgt_date AS fisc_day,
        cal.fisc_per AS fisc_yr_per,
        0 AS qty,
        0 AS units_sold,
        'NA' AS uom,
        'NA' AS acct_hier_desc,
        'NA' AS acct_hier_shrt_desc,
        tgt.cntry_cd,
        'NA' AS company_nm,
        null AS fert_flag,
        COALESCE(tgt.store_type, 'NA'::character varying) AS store_type,
        0 AS free_good_value,
        0 AS pre_apsc_cogs,
        0 AS package_cost,
        0 AS labour_cost,
        null AS valuation_class,
        tgt.target_type,
        tgt.ytd_target_fy AS target_value,
        tgt.ytd_target_amt AS target_amount
    FROM 
        (
            (
                SELECT DISTINCT itg_kr_tp_tracker_target.cntry_cd,
                    itg_kr_tp_tracker_target.country_name,
                    itg_kr_tp_tracker_target.crncy_cd,
                    itg_kr_tp_tracker_target.store_type,
                    itg_kr_tp_tracker_target.channel,
                    itg_kr_tp_tracker_target.sales_group_cd,
                    itg_kr_tp_tracker_target.sales_group_name,
                    itg_kr_tp_tracker_target.target_type,
                    itg_kr_tp_tracker_target.year as "year",
                    itg_kr_tp_tracker_target.tgt_date,
                    itg_kr_tp_tracker_target.ytd_target_fy,
                    itg_kr_tp_tracker_target.brand,
                    itg_kr_tp_tracker_target.target_category_code,
                    itg_kr_tp_tracker_target.ytd_target_amt
                FROM itg_kr_tp_tracker_target
                WHERE 
                    (
                        (
                            (itg_kr_tp_tracker_target.target_category_code)::text <> 'TP'::text
                        )
                        AND (
                            (itg_kr_tp_tracker_target.year)::double precision >= (
                                date_part(
                                    year,
                                    convert_timezone('UTC', current_timestamp())::timestamp without time zone
                                ) - (2)::double precision
                            )
                        )
                    )
            ) tgt
            LEFT JOIN itg_kr_sales_store_map cust ON 
            (
                (
                    COALESCE(
                        upper(
                            trim(
                                (tgt.sales_group_cd)::text,
                                ('NA'::character varying)::text
                            )
                        )
                    ,'NA') = COALESCE(
                        upper(
                            trim(
                                (cust.sales_group_code)::text,
                                ('NA'::character varying)::text
                            )
                        )
                    ,'NA')
                )
            )
            LEFT JOIN 
            (
                SELECT DISTINCT edw_intrm_calendar.cal_day,
                    edw_intrm_calendar.cal_mo_1,
                    edw_intrm_calendar.fisc_yr,
                    edw_intrm_calendar.fisc_per,
                    edw_intrm_calendar.pstng_per
                FROM edw_intrm_calendar
            ) cal ON ((tgt.tgt_date = cal.cal_day))
            LEFT JOIN exrt ON 
            (
                (
                    ((tgt.crncy_cd)::text = (exrt.from_crncy)::text)
                    AND (cal.fisc_per = exrt.fisc_per)
                )
            )
            LEFT JOIN prod_hier ON (
                (
                    (prod_hier.prod_hier_l4)::text = (tgt.brand)::text
                )
            )
        )
),
nts_target as 
(
    SELECT 
        'NTS_Target' AS identifier,
        'South Korea' AS cntry_key,
        'NA' AS cust_num,
        tgt.channel,
        'NA' AS prft_ctr,
        'NA' AS matl,
        'NA' AS matl_desc,
        'NA' AS mega_brnd_desc,
        tgt.prod_hier_l4 AS brnd_desc,
        COALESCE(
            prod_hier.gcph_franchise,
            'NA'::character varying
        ) AS franchise,
        'NA' AS prod_minor,
        tgt.sls_grp_cd AS sls_grp,
        tgt.sls_grp AS sls_grp_desc,
        tgt.sls_ofc_cd AS sls_ofc,
        tgt.sls_ofc_desc,
        'NA' AS category_1,
        'NA' AS categroy_2,
        'NA' AS platform_ca,
        0 AS amt_obj_crncy,
        tgt.crncy_cd AS obj_crncy_co_obj,
        'Not Available' AS edw_cust_nm,
        exrt.from_crncy,
        exrt.to_crncy,
        exrt.ex_rt_typ,
        exrt.ex_rt,
        'NA' AS brand_group,
        (
            (
                (
                    (
                        "substring"((tgt.fisc_yr_per)::text, 1, 4) || '-'::text
                    ) || "substring"((tgt.fisc_yr_per)::text, 6, 2)
                ) || '-01'::text
            )
        )::date AS fisc_day,
        tgt.fisc_yr_per,
        0 AS qty,
        0 AS units_sold,
        'NA' AS uom,
        'NA' AS acct_hier_desc,
        'NA' AS acct_hier_shrt_desc,
        tgt.ctry_cd AS cntry_cd,
        'NA' AS company_nm,
        null AS fert_flag,
        tgt.store_type,
        0 AS free_good_value,
        0 AS pre_apsc_cogs,
        0 AS package_cost,
        0 AS labour_cost,
        null AS valuation_class,
        tgt.target_type,
        tgt.target_amt AS target_value,
        0 AS target_amount
    FROM 
        (
            (
                SELECT itg_kr_sales_tgt.ctry_cd,
                    itg_kr_sales_tgt.crncy_cd,
                    itg_kr_sales_tgt.sls_ofc_cd,
                    itg_kr_sales_tgt.sls_ofc_desc,
                    itg_kr_sales_tgt.channel,
                    itg_kr_sales_tgt.store_type,
                    itg_kr_sales_tgt.sls_grp_cd,
                    itg_kr_sales_tgt.sls_grp,
                    itg_kr_sales_tgt.target_type,
                    itg_kr_sales_tgt.prod_hier_l2,
                    itg_kr_sales_tgt.prod_hier_l4,
                    itg_kr_sales_tgt.fisc_yr,
                    itg_kr_sales_tgt.fisc_yr_per,
                    sum(itg_kr_sales_tgt.target_amt) AS target_amt
                FROM itg_kr_sales_tgt
                WHERE (
                        (itg_kr_sales_tgt.fisc_yr)::double precision >= (
                            date_part(
                                year,
                                convert_timezone('UTC', current_timestamp())::timestamp without time zone
                            ) - (2)::double precision
                        )
                    )
                GROUP BY itg_kr_sales_tgt.ctry_cd,
                    itg_kr_sales_tgt.crncy_cd,
                    itg_kr_sales_tgt.sls_ofc_cd,
                    itg_kr_sales_tgt.sls_ofc_desc,
                    itg_kr_sales_tgt.channel,
                    itg_kr_sales_tgt.store_type,
                    itg_kr_sales_tgt.sls_grp_cd,
                    itg_kr_sales_tgt.sls_grp,
                    itg_kr_sales_tgt.target_type,
                    itg_kr_sales_tgt.prod_hier_l2,
                    itg_kr_sales_tgt.prod_hier_l4,
                    itg_kr_sales_tgt.fisc_yr,
                    itg_kr_sales_tgt.fisc_yr_per
            ) tgt
            LEFT JOIN exrt ON 
            (
                (
                    ((tgt.crncy_cd)::text = (exrt.from_crncy)::text)
                    AND (tgt.fisc_yr_per = exrt.fisc_per)
                )
            )
            LEFT JOIN  prod_hier ON 
            (
                (
                    (tgt.prod_hier_l4)::text = (prod_hier.prod_hier_l4)::text
                )
            )
        )
),
final as
(
    select * from sales_1
    union all
    select * from sales_south_korea
    union all
    select * from fg_target
    union all
    select * from fg_nts_target
    union all
    select * from nts_target

)
select * from final