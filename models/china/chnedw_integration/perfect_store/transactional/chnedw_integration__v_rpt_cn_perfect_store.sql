{{ 
    config
    (
    materialized='view'
    ) 
}}
with edw_perfect_store_source_msl as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_msl') }}
),
edw_perfect_store_must_stock_sku_list as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_must_stock_sku_list') }}
),
edw_perfect_store_store_hierarchy as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_store_hierarchy') }}
),
edw_vw_ps_weights as 
(
    select * from {{ ref('aspedw_integration__edw_vw_ps_weights') }}
),
edw_perfect_store_source_sos_display as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_sos_display') }}
),
edw_perfect_store_source_osa as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_sos_display') }}
),
edw_perfect_store_source_sos as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_sos') }}
),
edw_perfect_store_source_planogram as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_planogram') }}
),
edw_perfect_store_source_sos_shelf as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_sos_shelf') }}
),
edw_perfect_store_product_master as
(
     select * from {{ ref('chnedw_integration__edw_perfect_store_product_master') }}
),
EDW_PERFECT_STORE_SOURCE_PROM as
(
    select * from {{ ref('chnedw_integration__edw_perfect_store_source_prom') }}
),
union_1 as  (
                                    SELECT 'Merchandising_Response' AS dataset,
                                        epssm.sfa_store_code AS customerid,
                                        NULL AS salespersonid,
                                        NULL AS mrch_resp_startdt,
                                        NULL AS mrch_resp_enddt,
                                        NULL AS survey_enddate,
                                        NULL AS questiontext,
                                        NULL AS value,
                                        'true' AS mustcarryitem,
                                        CASE 
                                            WHEN (epssm.actual = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                THEN 'true'::CHARACTER VARYING
                                            ELSE 'false'::CHARACTER VARYING
                                            END AS presence,
                                        NULL AS outofstock,
                                        'MSL Compliance' AS kpi,
                                        to_date(((epssm.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                                        'completed' AS vst_status,
                                        (
                                            "substring" (
                                                (epssm.yyyymm)::TEXT,
                                                1,
                                                4
                                                )
                                            )::INTEGER AS fisc_yr,
                                        (epssm.yyyymm)::INTEGER AS fisc_per,
                                        epssh.rsm_name AS firstname,
                                        '' AS lastname,
                                        epssh.cus_name AS customername,
                                        'China' AS country,
                                        epssh.region AS STATE,
                                        epssh.ka_name AS storereference,
                                        epssh.store_type AS storetype,
                                        'MT' AS channel,
                                        epssh.distributor AS salesgroup,
                                        epssh.bu,
                                        'China' AS prod_hier_l1,
                                        (upper((epspm.brand)::TEXT))::CHARACTER VARYING AS prod_hier_l4,
                                        (upper((epspm.layer2)::TEXT))::CHARACTER VARYING AS prod_hier_l5,
                                        epspm.layer3 AS prod_hier_l6,
                                        epspm.product_name_en AS prod_hier_l9,
                                        epspm.product_name_en AS productname,
                                        epsmssl.upc AS eannumber,
                                        NULL AS category,
                                        NULL AS segment,
                                        icpw.weight AS kpi_chnl_wt,
                                        NULL AS actual,
                                        NULL AS target,
                                        NULL AS mkt_share,
                                        NULL AS ques_desc,
                                        NULL AS "y/n_flag",
                                        epssh.province,
                                        epssh.city,
                                        epssh.district,
                                        epssh.address,
                                        epssh.area,
                                        epssh.ka_type,
                                        epssh.store_property,
                                        epssh.store_g_d,
                                        epssh.supply_type,
                                        epssh.agent,
                                        epssh.customer_manager,
                                        epssh.sales_supervisor,
                                        epssh.asm_name
                                    FROM (
                                        (
                                            (
                                                (
                                                    (
                                                        SELECT edw_perfect_store_source_msl.sfa_store_code,
                                                            edw_perfect_store_source_msl.j_code,
                                                            edw_perfect_store_source_msl.re,
                                                            edw_perfect_store_source_msl.yyyymm,
                                                            edw_perfect_store_source_msl.brand,
                                                            edw_perfect_store_source_msl.sku,
                                                            edw_perfect_store_source_msl.PLAN,
                                                            edw_perfect_store_source_msl.actual,
                                                            edw_perfect_store_source_msl.update_time
                                                        FROM edw_perfect_store_source_msl
                                                        WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                        ) epssm JOIN (
                                                        SELECT DISTINCT derived_table1.sku_name,
                                                            derived_table1.sku_code,
                                                            derived_table1.upc
                                                        FROM (
                                                            SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                                                edw_perfect_store_must_stock_sku_list.sku_code,
                                                                edw_perfect_store_must_stock_sku_list.upc,
                                                                edw_perfect_store_must_stock_sku_list.update_time,
                                                                row_number() OVER (
                                                                    PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                                                    ) AS rn
                                                            FROM edw_perfect_store_must_stock_sku_list
                                                            ) derived_table1
                                                        WHERE (derived_table1.rn = 1)
                                                        ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                                                    ) LEFT JOIN (
                                                    SELECT DISTINCT derived_table2.yyyymm,
                                                        derived_table2.cus_code,
                                                        derived_table2.unit_code,
                                                        derived_table2.cus_name,
                                                        derived_table2.province,
                                                        derived_table2.city,
                                                        derived_table2.district,
                                                        derived_table2.address,
                                                        derived_table2.bu,
                                                        derived_table2.region,
                                                        derived_table2.area,
                                                        derived_table2.store_type,
                                                        derived_table2.ka_name,
                                                        derived_table2.ka_type,
                                                        derived_table2.store_property,
                                                        derived_table2.store_g_d,
                                                        derived_table2.supply_type,
                                                        derived_table2.agent,
                                                        derived_table2.distributor,
                                                        derived_table2.customer_manager,
                                                        derived_table2.sales_supervisor,
                                                        derived_table2.asm_name,
                                                        derived_table2.rsm_name,
                                                        derived_table2.update_time
                                                    FROM (
                                                        SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                                            edw_perfect_store_store_hierarchy.cus_code,
                                                            edw_perfect_store_store_hierarchy.unit_code,
                                                            edw_perfect_store_store_hierarchy.cus_name,
                                                            edw_perfect_store_store_hierarchy.province,
                                                            edw_perfect_store_store_hierarchy.city,
                                                            edw_perfect_store_store_hierarchy.district,
                                                            edw_perfect_store_store_hierarchy.address,
                                                            edw_perfect_store_store_hierarchy.bu,
                                                            edw_perfect_store_store_hierarchy.region,
                                                            edw_perfect_store_store_hierarchy.area,
                                                            edw_perfect_store_store_hierarchy.store_type,
                                                            edw_perfect_store_store_hierarchy.ka_name,
                                                            edw_perfect_store_store_hierarchy.ka_type,
                                                            edw_perfect_store_store_hierarchy.store_property,
                                                            edw_perfect_store_store_hierarchy.store_g_d,
                                                            edw_perfect_store_store_hierarchy.supply_type,
                                                            edw_perfect_store_store_hierarchy.agent,
                                                            edw_perfect_store_store_hierarchy.distributor,
                                                            edw_perfect_store_store_hierarchy.customer_manager,
                                                            edw_perfect_store_store_hierarchy.sales_supervisor,
                                                            edw_perfect_store_store_hierarchy.asm_name,
                                                            edw_perfect_store_store_hierarchy.rsm_name,
                                                            edw_perfect_store_store_hierarchy.update_time,
                                                            row_number() OVER (
                                                                PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                                                edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                                                ) AS row_num
                                                        FROM edw_perfect_store_store_hierarchy
                                                        ) derived_table2
                                                    WHERE (derived_table2.row_num = 1)
                                                    ) epssh ON (
                                                        (
                                                            (ltrim((epssm.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                                            AND ((epssm.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                                            )
                                                        )
                                                ) LEFT JOIN (
                                                SELECT DISTINCT edw_perfect_store_product_master.product_code,
                                                    edw_perfect_store_product_master.product_name_cn,
                                                    edw_perfect_store_product_master.product_name_en,
                                                    edw_perfect_store_product_master.brand,
                                                    edw_perfect_store_product_master.layer2,
                                                    edw_perfect_store_product_master.layer3
                                                FROM edw_perfect_store_product_master
                                                ) epspm ON ((ltrim((epsmssl.sku_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epspm.product_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT)))
                                            ) LEFT JOIN (
                                            SELECT edw_vw_ps_weights.retail_environment AS re,
                                                edw_vw_ps_weights.weight
                                            FROM edw_vw_ps_weights
                                            WHERE (
                                                    (
                                                        (upper((edw_vw_ps_weights.kpi)::TEXT) = ('MSL COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                                        AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                                    )
                                            ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                                        )
                                    WHERE (
                                            (
                                                (
                                                    "substring" (
                                                        (
                                                            CASE 
                                                                WHEN (
                                                                        ((epssm.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                                                        OR (
                                                                            (epssm.yyyymm IS NULL)
                                                                            AND ('' IS NULL)
                                                                            )
                                                                        )
                                                                    THEN NULL::CHARACTER VARYING
                                                                ELSE epssm.yyyymm
                                                                END
                                                            )::TEXT,
                                                        1,
                                                        4
                                                        )
                                                    )::INTEGER
                                                )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                                            )
                                    
                                    UNION ALL
                                    
                                    SELECT 'Merchandising_Response' AS dataset,
                                        epsso.sfa_store_code AS customerid,
                                        NULL AS salespersonid,
                                        NULL AS mrch_resp_startdt,
                                        NULL AS mrch_resp_enddt,
                                        NULL AS survey_enddate,
                                       NULL AS questiontext,
                                        NULL AS value,
                                        'true' AS mustcarryitem,
                                        'true' AS presence,
                                        CASE 
                                            WHEN (epsso.actual = (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                THEN 'true'::CHARACTER VARYING
                                            ELSE ''::CHARACTER VARYING
                                            END AS outofstock,
                                        'OOS Compliance' AS kpi,
                                        to_date(((epsso.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                                        'completed' AS vst_status,
                                        (
                                            "substring" (
                                                (epsso.yyyymm)::TEXT,
                                                1,
                                                4
                                                )
                                            )::INTEGER AS fisc_yr,
                                        (epsso.yyyymm)::INTEGER AS fisc_per,
                                        epssh.rsm_name AS firstname,
                                        '' AS lastname,
                                        epssh.cus_name AS customername,
                                        'China' AS country,
                                        epssh.region AS STATE,
                                        epssh.ka_name AS storereference,
                                        epssh.store_type AS storetype,
                                        'MT' AS channel,
                                        epssh.distributor AS salesgroup,
                                        epssh.bu,
                                        'China' AS prod_hier_l1,
                                        (upper((epspm.brand)::TEXT))::CHARACTER VARYING AS prod_hier_l4,
                                        (upper((epspm.layer2)::TEXT))::CHARACTER VARYING AS prod_hier_l5,
                                        epspm.layer3 AS prod_hier_l6,
                                        epspm.product_name_en AS prod_hier_l9,
                                        epspm.product_name_en AS productname,
                                        epsmssl.upc AS eannumber,
                                        NULL AS category,
                                        NULL AS segment,
                                        icpw.weight AS kpi_chnl_wt,
                                        NULL AS actual,
                                        NULL AS target,
                                        NULL AS mkt_share,
                                        NULL AS ques_desc,
                                        NULL AS "y/n_flag",
                                        epssh.province,
                                        epssh.city,
                                        epssh.district,
                                        epssh.address,
                                        epssh.area,
                                        epssh.ka_type,
                                        epssh.store_property,
                                        epssh.store_g_d,
                                        epssh.supply_type,
                                        epssh.agent,
                                        epssh.customer_manager,
                                        epssh.sales_supervisor,
                                        epssh.asm_name
                                    FROM (
                                        (
                                            (
                                                (
                                                    (
                                                        SELECT edw_perfect_store_source_osa.sfa_store_code,
                                                            edw_perfect_store_source_osa.j_code,
                                                            edw_perfect_store_source_osa.re,
                                                            edw_perfect_store_source_osa.yyyymm,
                                                            edw_perfect_store_source_osa.brand,
                                                            edw_perfect_store_source_osa.sku,
                                                            edw_perfect_store_source_osa.PLAN,
                                                            edw_perfect_store_source_osa.actual,
                                                            edw_perfect_store_source_osa.update_time
                                                        FROM edw_perfect_store_source_osa
                                                        WHERE (
                                                                (edw_perfect_store_source_osa.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                                AND (
                                                                    (((edw_perfect_store_source_osa.sfa_store_code)::TEXT || (edw_perfect_store_source_osa.sku)::TEXT) || (edw_perfect_store_source_osa.yyyymm)::TEXT) IN (
                                                                        SELECT DISTINCT (((edw_perfect_store_source_msl.sfa_store_code)::TEXT || (edw_perfect_store_source_msl.sku)::TEXT) || (edw_perfect_store_source_msl.yyyymm)::TEXT)
                                                                        FROM edw_perfect_store_source_msl
                                                                        WHERE (edw_perfect_store_source_msl.actual = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                                        )
                                                                    )
                                                                )
                                                        ) epsso JOIN (
                                                        SELECT DISTINCT derived_table3.sku_name,
                                                            derived_table3.sku_code,
                                                            derived_table3.upc
                                                        FROM (
                                                            SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                                                edw_perfect_store_must_stock_sku_list.sku_code,
                                                                edw_perfect_store_must_stock_sku_list.upc,
                                                                edw_perfect_store_must_stock_sku_list.update_time,
                                                                row_number() OVER (
                                                                    PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                                                    ) AS rn
                                                            FROM edw_perfect_store_must_stock_sku_list
                                                            ) derived_table3
                                                        WHERE (derived_table3.rn = 1)
                                                        ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epsso.sku)::TEXT)))
                                                    ) LEFT JOIN (
                                                    SELECT DISTINCT derived_table4.yyyymm,
                                                        derived_table4.cus_code,
                                                        derived_table4.unit_code,
                                                        derived_table4.cus_name,
                                                        derived_table4.province,
                                                        derived_table4.city,
                                                        derived_table4.district,
                                                        derived_table4.address,
                                                        derived_table4.bu,
                                                        derived_table4.region,
                                                        derived_table4.area,
                                                        derived_table4.store_type,
                                                        derived_table4.ka_name,
                                                        derived_table4.ka_type,
                                                        derived_table4.store_property,
                                                        derived_table4.store_g_d,
                                                        derived_table4.supply_type,
                                                        derived_table4.agent,
                                                        derived_table4.distributor,
                                                        derived_table4.customer_manager,
                                                        derived_table4.sales_supervisor,
                                                        derived_table4.asm_name,
                                                        derived_table4.rsm_name,
                                                        derived_table4.update_time
                                                    FROM (
                                                        SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                                            edw_perfect_store_store_hierarchy.cus_code,
                                                            edw_perfect_store_store_hierarchy.unit_code,
                                                            edw_perfect_store_store_hierarchy.cus_name,
                                                            edw_perfect_store_store_hierarchy.province,
                                                            edw_perfect_store_store_hierarchy.city,
                                                            edw_perfect_store_store_hierarchy.district,
                                                            edw_perfect_store_store_hierarchy.address,
                                                            edw_perfect_store_store_hierarchy.bu,
                                                            edw_perfect_store_store_hierarchy.region,
                                                            edw_perfect_store_store_hierarchy.area,
                                                            edw_perfect_store_store_hierarchy.store_type,
                                                            edw_perfect_store_store_hierarchy.ka_name,
                                                            edw_perfect_store_store_hierarchy.ka_type,
                                                            edw_perfect_store_store_hierarchy.store_property,
                                                            edw_perfect_store_store_hierarchy.store_g_d,
                                                            edw_perfect_store_store_hierarchy.supply_type,
                                                            edw_perfect_store_store_hierarchy.agent,
                                                            edw_perfect_store_store_hierarchy.distributor,
                                                            edw_perfect_store_store_hierarchy.customer_manager,
                                                            edw_perfect_store_store_hierarchy.sales_supervisor,
                                                            edw_perfect_store_store_hierarchy.asm_name,
                                                            edw_perfect_store_store_hierarchy.rsm_name,
                                                            edw_perfect_store_store_hierarchy.update_time,
                                                            row_number() OVER (
                                                                PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                                                edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                                                ) AS row_num
                                                        FROM edw_perfect_store_store_hierarchy
                                                        ) derived_table4
                                                    WHERE (derived_table4.row_num = 1)
                                                    ) epssh ON (
                                                        (
                                                            (ltrim((epsso.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                                            AND ((epsso.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                                            )
                                                        )
                                                ) LEFT JOIN (
                                                SELECT DISTINCT edw_perfect_store_product_master.product_code,
                                                    edw_perfect_store_product_master.product_name_cn,
                                                    edw_perfect_store_product_master.product_name_en,
                                                    edw_perfect_store_product_master.brand,
                                                    edw_perfect_store_product_master.layer2,
                                                    edw_perfect_store_product_master.layer3
                                                FROM edw_perfect_store_product_master
                                                ) epspm ON ((ltrim((epsmssl.sku_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epspm.product_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT)))
                                            ) LEFT JOIN (
                                            SELECT edw_vw_ps_weights.retail_environment AS re,
                                                edw_vw_ps_weights.weight
                                            FROM edw_vw_ps_weights
                                            WHERE (
                                                    (
                                                        (upper((edw_vw_ps_weights.kpi)::TEXT) = ('OSA COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                                        AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                                    )
                                            ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                                        )
                                    WHERE (
                                            (
                                                (
                                                    "substring" (
                                                        (
                                                            CASE 
                                                                WHEN (
                                                                        ((epsso.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                                                        OR (
                                                                            (epsso.yyyymm IS NULL)
                                                                            AND ('' IS NULL)
                                                                            )
                                                                        )
                                                                    THEN NULL::CHARACTER VARYING
                                                                ELSE epsso.yyyymm
                                                                END
                                                            )::TEXT,
                                                        1,
                                                        4
                                                        )
                                                    )::INTEGER
                                                )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                                            )
                                    ),

union_2 as                            (
                               
                                
                                
                                SELECT 'Survey_Response' AS dataset,
                                    epssp.sfa_store_code AS customerid,
                                    NULL AS salespersonid,
                                    NULL AS mrch_resp_startdt,
                                    NULL AS mrch_resp_enddt,
                                    NULL AS survey_enddate,
                                    NULL AS questiontext,
                                    NULL AS value,
                                    NULL AS mustcarryitem,
                                    NULL AS presence,
                                    NULL AS outofstock,
                                    'Planogram Compliance' AS kpi,
                                    to_date(((epssp.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                                    NULL AS vst_status,
                                    (
                                        "substring" (
                                            (epssp.yyyymm)::TEXT,
                                            1,
                                            4
                                            )
                                        )::INTEGER AS fisc_yr,
                                    (epssp.yyyymm)::INTEGER AS fisc_per,
                                    epssh.rsm_name AS firstname,
                                    '' AS lastname,
                                    epssh.cus_name AS customername,
                                    'China' AS country,
                                    epssh.region AS STATE,
                                    epssh.ka_name AS storereference,
                                    epssh.store_type AS storetype,
                                    'MT' AS channel,
                                    epssh.distributor AS salesgroup,
                                    epssh.bu,
                                    NULL AS prod_hier_l1,
                                    NULL AS prod_hier_l4,
                                    NULL AS prod_hier_l5,
                                    NULL AS prod_hier_l6,
                                    NULL AS prod_hier_l9,
                                    NULL AS productname,
                                    NULL AS eannumber,
                                    (upper((epssp.brand)::TEXT))::CHARACTER VARYING AS category,
                                    NULL AS segment,
                                    icpw.weight AS kpi_chnl_wt,
                                    (epssp.actual)::CHARACTER VARYING AS actual,
                                    (epssp.PLAN)::CHARACTER VARYING AS target,
                                    NULL AS mkt_share,
                                    NULL AS ques_desc,
                                    NULL AS "y/n_flag",
                                    epssh.province,
                                    epssh.city,
                                    epssh.district,
                                    epssh.address,
                                    epssh.area,
                                    epssh.ka_type,
                                    epssh.store_property,
                                    epssh.store_g_d,
                                    epssh.supply_type,
                                    epssh.agent,
                                    epssh.customer_manager,
                                    epssh.sales_supervisor,
                                    epssh.asm_name
                                FROM (
                                    (
                                        (
                                            (
                                                SELECT edw_perfect_store_source_planogram.sfa_store_code,
                                                    edw_perfect_store_source_planogram.j_code,
                                                    edw_perfect_store_source_planogram.re,
                                                    edw_perfect_store_source_planogram.yyyymm,
                                                    edw_perfect_store_source_planogram.brand,
                                                    edw_perfect_store_source_planogram.PLAN,
                                                    edw_perfect_store_source_planogram.actual,
                                                    edw_perfect_store_source_planogram.update_time
                                                FROM edw_perfect_store_source_planogram
                                                WHERE (
                                                        (edw_perfect_store_source_planogram.PLAN IS NOT NULL)
                                                        AND (edw_perfect_store_source_planogram.PLAN <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                        )
                                                ) epssp LEFT JOIN (
                                                SELECT DISTINCT derived_table5.yyyymm,
                                                    derived_table5.cus_code,
                                                    derived_table5.unit_code,
                                                    derived_table5.cus_name,
                                                    derived_table5.province,
                                                    derived_table5.city,
                                                    derived_table5.district,
                                                    derived_table5.address,
                                                    derived_table5.bu,
                                                    derived_table5.region,
                                                    derived_table5.area,
                                                    derived_table5.store_type,
                                                    derived_table5.ka_name,
                                                    derived_table5.ka_type,
                                                    derived_table5.store_property,
                                                    derived_table5.store_g_d,
                                                    derived_table5.supply_type,
                                                    derived_table5.agent,
                                                    derived_table5.distributor,
                                                    derived_table5.customer_manager,
                                                    derived_table5.sales_supervisor,
                                                    derived_table5.asm_name,
                                                    derived_table5.rsm_name,
                                                    derived_table5.update_time
                                                FROM (
                                                    SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                                        edw_perfect_store_store_hierarchy.cus_code,
                                                        edw_perfect_store_store_hierarchy.unit_code,
                                                        edw_perfect_store_store_hierarchy.cus_name,
                                                        edw_perfect_store_store_hierarchy.province,
                                                        edw_perfect_store_store_hierarchy.city,
                                                        edw_perfect_store_store_hierarchy.district,
                                                        edw_perfect_store_store_hierarchy.address,
                                                        edw_perfect_store_store_hierarchy.bu,
                                                        edw_perfect_store_store_hierarchy.region,
                                                        edw_perfect_store_store_hierarchy.area,
                                                        edw_perfect_store_store_hierarchy.store_type,
                                                        edw_perfect_store_store_hierarchy.ka_name,
                                                        edw_perfect_store_store_hierarchy.ka_type,
                                                        edw_perfect_store_store_hierarchy.store_property,
                                                        edw_perfect_store_store_hierarchy.store_g_d,
                                                        edw_perfect_store_store_hierarchy.supply_type,
                                                        edw_perfect_store_store_hierarchy.agent,
                                                        edw_perfect_store_store_hierarchy.distributor,
                                                        edw_perfect_store_store_hierarchy.customer_manager,
                                                        edw_perfect_store_store_hierarchy.sales_supervisor,
                                                        edw_perfect_store_store_hierarchy.asm_name,
                                                        edw_perfect_store_store_hierarchy.rsm_name,
                                                        edw_perfect_store_store_hierarchy.update_time,
                                                        row_number() OVER (
                                                            PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                                            edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                                            ) AS row_num
                                                    FROM edw_perfect_store_store_hierarchy
                                                    ) derived_table5
                                                WHERE (derived_table5.row_num = 1)
                                                ) epssh ON (
                                                    (
                                                        (ltrim((epssp.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                                        AND ((epssp.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                                        )
                                                    )
                                            ) LEFT JOIN (
                                            SELECT edw_vw_ps_weights.retail_environment AS re,
                                                edw_vw_ps_weights.weight
                                            FROM edw_vw_ps_weights
                                            WHERE (
                                                    (
                                                        (upper((edw_vw_ps_weights.kpi)::TEXT) = ('PLANOGRAM COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                                        AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                                        )
                                                    AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                                    )
                                            ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                                        ) JOIN (
                                        SELECT DISTINCT epssm.yyyymm,
                                            epssm.sfa_store_code
                                        FROM (
                                            (
                                                SELECT edw_perfect_store_source_msl.sfa_store_code,
                                                    edw_perfect_store_source_msl.j_code,
                                                    edw_perfect_store_source_msl.re,
                                                    edw_perfect_store_source_msl.yyyymm,
                                                    edw_perfect_store_source_msl.brand,
                                                    edw_perfect_store_source_msl.sku,
                                                    edw_perfect_store_source_msl.PLAN,
                                                    edw_perfect_store_source_msl.actual,
                                                    edw_perfect_store_source_msl.update_time
                                                FROM edw_perfect_store_source_msl
                                                WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                ) epssm JOIN (
                                                SELECT DISTINCT derived_table6.sku_name,
                                                    derived_table6.sku_code,
                                                    derived_table6.upc
                                                FROM (
                                                    SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                                        edw_perfect_store_must_stock_sku_list.sku_code,
                                                        edw_perfect_store_must_stock_sku_list.upc,
                                                        edw_perfect_store_must_stock_sku_list.update_time,
                                                        row_number() OVER (
                                                            PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                                            ) AS rn
                                                    FROM edw_perfect_store_must_stock_sku_list
                                                    ) derived_table6
                                                WHERE (derived_table6.rn = 1)
                                                ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                                            )
                                        ) msl ON (
                                            (
                                                (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssp.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                                AND ((msl.yyyymm)::TEXT = (epssp.yyyymm)::TEXT)
                                                )
                                            )
                                    )
                                WHERE (
                                        (
                                            (
                                                "substring" (
                                                    (
                                                        CASE 
                                                            WHEN (
                                                                    ((epssp.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                                                    OR (
                                                                        (epssp.yyyymm IS NULL)
                                                                        AND ('' IS NULL)
                                                                        )
                                                                    )
                                                                THEN NULL::CHARACTER VARYING
                                                            ELSE epssp.yyyymm
                                                            END
                                                        )::TEXT,
                                                    1,
                                                    4
                                                    )
                                                )::INTEGER
                                            )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                                        )
                                ),

union_3 as (
                             
                            SELECT 'Survey_Response' AS dataset,
                                epsss.customer_code AS customerid,
                                NULL AS salespersonid,
                                NULL AS mrch_resp_startdt,
                                NULL AS mrch_resp_enddt,
                                NULL AS survey_enddate,
                                NULL AS questiontext,
                                (
                                    CASE 
                                        WHEN (epsss.store_actual_num = (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                            THEN 0
                                        ELSE 1
                                        END
                                    )::CHARACTER VARYING AS value,
                                NULL AS mustcarryitem,
                                NULL AS presence,
                                NULL AS outofstock,
                                'Share of Shelf' AS kpi,
                                to_date(((epsss.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                                NULL AS vst_status,
                                (
                                    "substring" (
                                        (epsss.yyyymm)::TEXT,
                                        1,
                                        4
                                        )
                                    )::INTEGER AS fisc_yr,
                                (epsss.yyyymm)::INTEGER AS fisc_per,
                                epssh.rsm_name AS firstname,
                                '' AS lastname,
                                epssh.cus_name AS customername,
                                'China' AS country,
                                epssh.region AS STATE,
                                epssh.ka_name AS storereference,
                                epssh.store_type AS storetype,
                                'MT' AS channel,
                                epssh.distributor AS salesgroup,
                                epssh.bu,
                                NULL AS prod_hier_l1,
                                NULL AS prod_hier_l4,
                                NULL AS prod_hier_l5,
                                NULL AS prod_hier_l6,
                                NULL AS prod_hier_l9,
                                NULL AS productname,
                                NULL AS eannumber,
                                (upper((epsss.brand)::TEXT))::CHARACTER VARYING AS category,
                                NULL AS segment,
                                icpw.weight AS kpi_chnl_wt,
                                NULL AS actual,
                                NULL AS target,
                                epsss.shelf_num AS mkt_share,
                                'numerator' AS ques_desc,
                                NULL AS "y/n_flag",
                                epssh.province,
                                epssh.city,
                                epssh.district,
                                epssh.address,
                                epssh.area,
                                epssh.ka_type,
                                epssh.store_property,
                                epssh.store_g_d,
                                epssh.supply_type,
                                epssh.agent,
                                epssh.customer_manager,
                                epssh.sales_supervisor,
                                epssh.asm_name
                            FROM (
                                (
                                    (
                                        (
                                            SELECT edw_perfect_store_source_sos.customer_code,
                                                edw_perfect_store_source_sos.unit_code,
                                                edw_perfect_store_source_sos.visit_customer_name,
                                                edw_perfect_store_source_sos.re,
                                                edw_perfect_store_source_sos.yyyymm,
                                                edw_perfect_store_source_sos.brand,
                                                edw_perfect_store_source_sos.shelf_num,
                                                edw_perfect_store_source_sos.store_actual_num,
                                                edw_perfect_store_source_sos.update_time
                                            FROM edw_perfect_store_source_sos
                                            WHERE (
                                                    (
                                                        (
                                                            (edw_perfect_store_source_sos.shelf_num IS NOT NULL)
                                                            AND (
                                                                CASE 
                                                                    WHEN (trim((edw_perfect_store_source_sos.yyyymm)::TEXT) = (''::CHARACTER VARYING)::TEXT)
                                                                        THEN (NULL::CHARACTER VARYING)::TEXT
                                                                    ELSE trim((edw_perfect_store_source_sos.yyyymm)::TEXT)
                                                                    END IS NOT NULL
                                                                )
                                                            )
                                                        AND (edw_perfect_store_source_sos.store_actual_num IS NOT NULL)
                                                        )
                                                    AND (edw_perfect_store_source_sos.shelf_num <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                    )
                                            ) epsss LEFT JOIN (
                                            SELECT DISTINCT derived_table7.yyyymm,
                                                derived_table7.cus_code,
                                                derived_table7.unit_code,
                                                derived_table7.cus_name,
                                                derived_table7.province,
                                                derived_table7.city,
                                                derived_table7.district,
                                                derived_table7.address,
                                                derived_table7.bu,
                                                derived_table7.region,
                                                derived_table7.area,
                                                derived_table7.store_type,
                                                derived_table7.ka_name,
                                                derived_table7.ka_type,
                                                derived_table7.store_property,
                                                derived_table7.store_g_d,
                                                derived_table7.supply_type,
                                                derived_table7.agent,
                                                derived_table7.distributor,
                                                derived_table7.customer_manager,
                                                derived_table7.sales_supervisor,
                                                derived_table7.asm_name,
                                                derived_table7.rsm_name,
                                                derived_table7.update_time
                                            FROM (
                                                SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                                    edw_perfect_store_store_hierarchy.cus_code,
                                                    edw_perfect_store_store_hierarchy.unit_code,
                                                    edw_perfect_store_store_hierarchy.cus_name,
                                                    edw_perfect_store_store_hierarchy.province,
                                                    edw_perfect_store_store_hierarchy.city,
                                                    edw_perfect_store_store_hierarchy.district,
                                                    edw_perfect_store_store_hierarchy.address,
                                                    edw_perfect_store_store_hierarchy.bu,
                                                    edw_perfect_store_store_hierarchy.region,
                                                    edw_perfect_store_store_hierarchy.area,
                                                    edw_perfect_store_store_hierarchy.store_type,
                                                    edw_perfect_store_store_hierarchy.ka_name,
                                                    edw_perfect_store_store_hierarchy.ka_type,
                                                    edw_perfect_store_store_hierarchy.store_property,
                                                    edw_perfect_store_store_hierarchy.store_g_d,
                                                    edw_perfect_store_store_hierarchy.supply_type,
                                                    edw_perfect_store_store_hierarchy.agent,
                                                    edw_perfect_store_store_hierarchy.distributor,
                                                    edw_perfect_store_store_hierarchy.customer_manager,
                                                    edw_perfect_store_store_hierarchy.sales_supervisor,
                                                    edw_perfect_store_store_hierarchy.asm_name,
                                                    edw_perfect_store_store_hierarchy.rsm_name,
                                                    edw_perfect_store_store_hierarchy.update_time,
                                                    row_number() OVER (
                                                        PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                                        edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                                        ) AS row_num
                                                FROM edw_perfect_store_store_hierarchy
                                                ) derived_table7
                                            WHERE (derived_table7.row_num = 1)
                                            ) epssh ON (
                                                (
                                                    (ltrim((epsss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                                    AND ((epsss.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                                    )
                                                )
                                        ) LEFT JOIN (
                                        SELECT edw_vw_ps_weights.retail_environment AS re,
                                            edw_vw_ps_weights.weight
                                        FROM edw_vw_ps_weights
                                        WHERE (
                                                (
                                                    (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                                    AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                                    )
                                                AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                                )
                                        ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                                    ) JOIN (
                                    SELECT DISTINCT epssm.yyyymm,
                                        epssm.sfa_store_code
                                    FROM (
                                        (
                                            SELECT edw_perfect_store_source_msl.sfa_store_code,
                                                edw_perfect_store_source_msl.j_code,
                                                edw_perfect_store_source_msl.re,
                                                edw_perfect_store_source_msl.yyyymm,
                                                edw_perfect_store_source_msl.brand,
                                                edw_perfect_store_source_msl.sku,
                                                edw_perfect_store_source_msl.PLAN,
                                                edw_perfect_store_source_msl.actual,
                                                edw_perfect_store_source_msl.update_time
                                            FROM edw_perfect_store_source_msl
                                            WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                            ) epssm JOIN (
                                            SELECT DISTINCT derived_table8.sku_name,
                                                derived_table8.sku_code,
                                                derived_table8.upc
                                            FROM (
                                                SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                                    edw_perfect_store_must_stock_sku_list.sku_code,
                                                    edw_perfect_store_must_stock_sku_list.upc,
                                                    edw_perfect_store_must_stock_sku_list.update_time,
                                                    row_number() OVER (
                                                        PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                                        ) AS rn
                                                FROM edw_perfect_store_must_stock_sku_list
                                                ) derived_table8
                                            WHERE (derived_table8.rn = 1)
                                            ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                                        )
                                    ) msl ON (
                                        (
                                            (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epsss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                            AND ((msl.yyyymm)::TEXT = (epsss.yyyymm)::TEXT)
                                            )
                                        )
                                )
                            WHERE (date_part((year), (to_date((epsss.yyyymm),('YYYYMM')))) >= (DATE_PART(year,dateadd(day,-2,sysdate()))))
                            ),
union_4 as (
                        
                       
                        
                        SELECT 'Survey_Response' AS dataset,
                            epsss.customer_code AS customerid,
                            NULL AS salespersonid,
                            NULL AS mrch_resp_startdt,
                            NULL AS mrch_resp_enddt,
                            NULL AS survey_enddate,
                            NULL AS questiontext,
                            (
                                (
                                    CASE 
                                        WHEN (epsss.store_actual_num = (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                            THEN ((1)::NUMERIC)::NUMERIC(18, 0)
                                        ELSE (((1)::NUMERIC)::NUMERIC(18, 0) / epsss.store_actual_num)
                                        END
                                    )::NUMERIC(20, 3)
                                )::CHARACTER VARYING AS value,
                            NULL AS mustcarryitem,
                            NULL AS presence,
                            NULL AS outofstock,
                            'Share of Shelf' AS kpi,
                            to_date(((epsss.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                            NULL AS vst_status,
                            (
                                "substring" (
                                    (epsss.yyyymm)::TEXT,
                                    1,
                                    4
                                    )
                                )::INTEGER AS fisc_yr,
                            (epsss.yyyymm)::INTEGER AS fisc_per,
                            epssh.rsm_name AS firstname,
                            '' AS lastname,
                            epssh.cus_name AS customername,
                            'China' AS country,
                            epssh.region AS STATE,
                            epssh.ka_name AS storereference,
                            epssh.store_type AS storetype,
                            'MT' AS channel,
                            epssh.distributor AS salesgroup,
                            epssh.bu,
                            NULL AS prod_hier_l1,
                            NULL AS prod_hier_l4,
                            NULL AS prod_hier_l5,
                            NULL AS prod_hier_l6,
                            NULL AS prod_hier_l9,
                            NULL AS productname,
                            NULL AS eannumber,
                            (upper((epsss.brand)::TEXT))::CHARACTER VARYING AS category,
                            NULL AS segment,
                            icpw.weight AS kpi_chnl_wt,
                            NULL AS actual,
                            NULL AS target,
                            epsss.shelf_num AS mkt_share,
                            'denominator' AS ques_desc,
                            NULL AS "y/n_flag",
                            epssh.province,
                            epssh.city,
                            epssh.district,
                            epssh.address,
                            epssh.area,
                            epssh.ka_type,
                            epssh.store_property,
                            epssh.store_g_d,
                            epssh.supply_type,
                            epssh.agent,
                            epssh.customer_manager,
                            epssh.sales_supervisor,
                            epssh.asm_name
                        FROM (
                            (
                                (
                                    (
                                        SELECT edw_perfect_store_source_sos.customer_code,
                                            edw_perfect_store_source_sos.unit_code,
                                            edw_perfect_store_source_sos.visit_customer_name,
                                            edw_perfect_store_source_sos.re,
                                            edw_perfect_store_source_sos.yyyymm,
                                            edw_perfect_store_source_sos.brand,
                                            edw_perfect_store_source_sos.shelf_num,
                                            edw_perfect_store_source_sos.store_actual_num,
                                            edw_perfect_store_source_sos.update_time
                                        FROM edw_perfect_store_source_sos
                                        WHERE (
                                                (
                                                    (
                                                        (edw_perfect_store_source_sos.shelf_num IS NOT NULL)
                                                        AND (
                                                            CASE 
                                                                WHEN (trim((edw_perfect_store_source_sos.yyyymm)::TEXT) = (''::CHARACTER VARYING)::TEXT)
                                                                    THEN (NULL::CHARACTER VARYING)::TEXT
                                                                ELSE trim((edw_perfect_store_source_sos.yyyymm)::TEXT)
                                                                END IS NOT NULL
                                                            )
                                                        )
                                                    AND (edw_perfect_store_source_sos.store_actual_num IS NOT NULL)
                                                    )
                                                AND (edw_perfect_store_source_sos.shelf_num <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                                )
                                        ) epsss LEFT JOIN (
                                        SELECT DISTINCT derived_table9.yyyymm,
                                            derived_table9.cus_code,
                                            derived_table9.unit_code,
                                            derived_table9.cus_name,
                                            derived_table9.province,
                                            derived_table9.city,
                                            derived_table9.district,
                                            derived_table9.address,
                                            derived_table9.bu,
                                            derived_table9.region,
                                            derived_table9.area,
                                            derived_table9.store_type,
                                            derived_table9.ka_name,
                                            derived_table9.ka_type,
                                            derived_table9.store_property,
                                            derived_table9.store_g_d,
                                            derived_table9.supply_type,
                                            derived_table9.agent,
                                            derived_table9.distributor,
                                            derived_table9.customer_manager,
                                            derived_table9.sales_supervisor,
                                            derived_table9.asm_name,
                                            derived_table9.rsm_name,
                                            derived_table9.update_time
                                        FROM (
                                            SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                                edw_perfect_store_store_hierarchy.cus_code,
                                                edw_perfect_store_store_hierarchy.unit_code,
                                                edw_perfect_store_store_hierarchy.cus_name,
                                                edw_perfect_store_store_hierarchy.province,
                                                edw_perfect_store_store_hierarchy.city,
                                                edw_perfect_store_store_hierarchy.district,
                                                edw_perfect_store_store_hierarchy.address,
                                                edw_perfect_store_store_hierarchy.bu,
                                                edw_perfect_store_store_hierarchy.region,
                                                edw_perfect_store_store_hierarchy.area,
                                                edw_perfect_store_store_hierarchy.store_type,
                                                edw_perfect_store_store_hierarchy.ka_name,
                                                edw_perfect_store_store_hierarchy.ka_type,
                                                edw_perfect_store_store_hierarchy.store_property,
                                                edw_perfect_store_store_hierarchy.store_g_d,
                                                edw_perfect_store_store_hierarchy.supply_type,
                                                edw_perfect_store_store_hierarchy.agent,
                                                edw_perfect_store_store_hierarchy.distributor,
                                                edw_perfect_store_store_hierarchy.customer_manager,
                                                edw_perfect_store_store_hierarchy.sales_supervisor,
                                                edw_perfect_store_store_hierarchy.asm_name,
                                                edw_perfect_store_store_hierarchy.rsm_name,
                                                edw_perfect_store_store_hierarchy.update_time,
                                                row_number() OVER (
                                                    PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                                    edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                                    ) AS row_num
                                            FROM edw_perfect_store_store_hierarchy
                                            ) derived_table9
                                        WHERE (derived_table9.row_num = 1)
                                        ) epssh ON (
                                            (
                                                (ltrim((epsss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                                AND ((epsss.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                                )
                                            )
                                    ) LEFT JOIN (
                                    SELECT edw_vw_ps_weights.retail_environment AS re,
                                        edw_vw_ps_weights.weight
                                    FROM edw_vw_ps_weights
                                    WHERE (
                                            (
                                                (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                                AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                                )
                                            AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                            )
                                    ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                                ) JOIN (
                                SELECT DISTINCT epssm.yyyymm,
                                    epssm.sfa_store_code
                                FROM (
                                    (
                                        SELECT edw_perfect_store_source_msl.sfa_store_code,
                                            edw_perfect_store_source_msl.j_code,
                                            edw_perfect_store_source_msl.re,
                                            edw_perfect_store_source_msl.yyyymm,
                                            edw_perfect_store_source_msl.brand,
                                            edw_perfect_store_source_msl.sku,
                                            edw_perfect_store_source_msl.PLAN,
                                            edw_perfect_store_source_msl.actual,
                                            edw_perfect_store_source_msl.update_time
                                        FROM edw_perfect_store_source_msl
                                        WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                        ) epssm JOIN (
                                        SELECT DISTINCT derived_table10.sku_name,
                                            derived_table10.sku_code,
                                            derived_table10.upc
                                        FROM (
                                            SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                                edw_perfect_store_must_stock_sku_list.sku_code,
                                                edw_perfect_store_must_stock_sku_list.upc,
                                                edw_perfect_store_must_stock_sku_list.update_time,
                                                row_number() OVER (
                                                    PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                                    ) AS rn
                                            FROM edw_perfect_store_must_stock_sku_list
                                            ) derived_table10
                                        WHERE (derived_table10.rn = 1)
                                        ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                                    )
                                ) msl ON (
                                    (
                                        (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epsss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                        AND ((msl.yyyymm)::TEXT = (epsss.yyyymm)::TEXT)
                                        )
                                    )
                            )
                        WHERE (date_part((year), (to_date((epsss.yyyymm),('YYYYMM')))) >= (DATE_PART(year,dateadd(day,-2,sysdate()))))
                        ),
union_5 as (
                    
                    
                    
                    SELECT 'Survey_Response' AS dataset,
                        epsssd.customer_code AS customerid,
                        NULL AS salespersonid,
                        NULL AS mrch_resp_startdt,
                        NULL AS mrch_resp_enddt,
                        NULL AS survey_enddate,
                        NULL AS questiontext,
                        (epsssd.display_fk)::CHARACTER VARYING AS value,
                        NULL AS mustcarryitem,
                        NULL AS presence,
                        NULL AS outofstock,
                        'Share of Shelf' AS kpi,
                        to_date(((epsssd.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                        NULL AS vst_status,
                        (
                            "substring" (
                                (epsssd.yyyymm)::TEXT,
                                1,
                                4
                                )
                            )::INTEGER AS fisc_yr,
                        (epsssd.yyyymm)::INTEGER AS fisc_per,
                        epssh.rsm_name AS firstname,
                        '' AS lastname,
                        epssh.cus_name AS customername,
                        'China' AS country,
                        epssh.region AS STATE,
                        epssh.ka_name AS storereference,
                        epssh.store_type AS storetype,
                        'MT' AS channel,
                        epssh.distributor AS salesgroup,
                        epssh.bu,
                        NULL AS prod_hier_l1,
                        NULL AS prod_hier_l4,
                        NULL AS prod_hier_l5,
                        NULL AS prod_hier_l6,
                        NULL AS prod_hier_l9,
                        NULL AS productname,
                        NULL AS eannumber,
                        (upper((epsssd.brand)::TEXT))::CHARACTER VARYING AS category,
                        NULL AS segment,
                        icpw.weight AS kpi_chnl_wt,
                        NULL AS actual,
                        NULL AS target,
                        1 AS mkt_share,
                        'numerator' AS ques_desc,
                        NULL AS "y/n_flag",
                        epssh.province,
                        epssh.city,
                        epssh.district,
                        epssh.address,
                        epssh.area,
                        epssh.ka_type,
                        epssh.store_property,
                        epssh.store_g_d,
                        epssh.supply_type,
                        epssh.agent,
                        epssh.customer_manager,
                        epssh.sales_supervisor,
                        epssh.asm_name
                    FROM (
                        (
                            (
                                (
                                    SELECT edw_perfect_store_source_sos_display.customer_code,
                                        edw_perfect_store_source_sos_display.unit_code,
                                        edw_perfect_store_source_sos_display.visit_customer_name,
                                        edw_perfect_store_source_sos_display.re,
                                        edw_perfect_store_source_sos_display.yyyymm,
                                        edw_perfect_store_source_sos_display.brand,
                                        edw_perfect_store_source_sos_display.display_yq,
                                        edw_perfect_store_source_sos_display.display_fk,
                                        edw_perfect_store_source_sos_display.update_time
                                    FROM edw_perfect_store_source_sos_display
                                    WHERE (
                                            (
                                                (edw_perfect_store_source_sos_display.display_yq IS NOT NULL)
                                                AND (edw_perfect_store_source_sos_display.display_fk IS NOT NULL)
                                                )
                                            AND (edw_perfect_store_source_sos_display.display_yq <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                            )
                                    ) epsssd LEFT JOIN (
                                    SELECT DISTINCT derived_table11.yyyymm,
                                        derived_table11.cus_code,
                                        derived_table11.unit_code,
                                        derived_table11.cus_name,
                                        derived_table11.province,
                                        derived_table11.city,
                                        derived_table11.district,
                                        derived_table11.address,
                                        derived_table11.bu,
                                        derived_table11.region,
                                        derived_table11.area,
                                        derived_table11.store_type,
                                        derived_table11.ka_name,
                                        derived_table11.ka_type,
                                        derived_table11.store_property,
                                        derived_table11.store_g_d,
                                        derived_table11.supply_type,
                                        derived_table11.agent,
                                        derived_table11.distributor,
                                        derived_table11.customer_manager,
                                        derived_table11.sales_supervisor,
                                        derived_table11.asm_name,
                                        derived_table11.rsm_name,
                                        derived_table11.update_time
                                    FROM (
                                        SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                            edw_perfect_store_store_hierarchy.cus_code,
                                            edw_perfect_store_store_hierarchy.unit_code,
                                            edw_perfect_store_store_hierarchy.cus_name,
                                            edw_perfect_store_store_hierarchy.province,
                                            edw_perfect_store_store_hierarchy.city,
                                            edw_perfect_store_store_hierarchy.district,
                                            edw_perfect_store_store_hierarchy.address,
                                            edw_perfect_store_store_hierarchy.bu,
                                            edw_perfect_store_store_hierarchy.region,
                                            edw_perfect_store_store_hierarchy.area,
                                            edw_perfect_store_store_hierarchy.store_type,
                                            edw_perfect_store_store_hierarchy.ka_name,
                                            edw_perfect_store_store_hierarchy.ka_type,
                                            edw_perfect_store_store_hierarchy.store_property,
                                            edw_perfect_store_store_hierarchy.store_g_d,
                                            edw_perfect_store_store_hierarchy.supply_type,
                                            edw_perfect_store_store_hierarchy.agent,
                                            edw_perfect_store_store_hierarchy.distributor,
                                            edw_perfect_store_store_hierarchy.customer_manager,
                                            edw_perfect_store_store_hierarchy.sales_supervisor,
                                            edw_perfect_store_store_hierarchy.asm_name,
                                            edw_perfect_store_store_hierarchy.rsm_name,
                                            edw_perfect_store_store_hierarchy.update_time,
                                            row_number() OVER (
                                                PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                                edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                                ) AS row_num
                                        FROM edw_perfect_store_store_hierarchy
                                        ) derived_table11
                                    WHERE (derived_table11.row_num = 1)
                                    ) epssh ON (
                                        (
                                            (ltrim((epsssd.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                            AND ((epsssd.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                            )
                                        )
                                ) LEFT JOIN (
                                SELECT edw_vw_ps_weights.retail_environment AS re,
                                    edw_vw_ps_weights.weight
                                FROM edw_vw_ps_weights
                                WHERE (
                                        (
                                            (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                            AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                            )
                                        AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                        )
                                ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                            ) JOIN (
                            SELECT DISTINCT epssm.yyyymm,
                                epssm.sfa_store_code
                            FROM (
                                (
                                    SELECT edw_perfect_store_source_msl.sfa_store_code,
                                        edw_perfect_store_source_msl.j_code,
                                        edw_perfect_store_source_msl.re,
                                        edw_perfect_store_source_msl.yyyymm,
                                        edw_perfect_store_source_msl.brand,
                                        edw_perfect_store_source_msl.sku,
                                        edw_perfect_store_source_msl.PLAN,
                                        edw_perfect_store_source_msl.actual,
                                        edw_perfect_store_source_msl.update_time
                                    FROM edw_perfect_store_source_msl
                                    WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                    ) epssm JOIN (
                                    SELECT DISTINCT derived_table12.sku_name,
                                        derived_table12.sku_code,
                                        derived_table12.upc
                                    FROM (
                                        SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                            edw_perfect_store_must_stock_sku_list.sku_code,
                                            edw_perfect_store_must_stock_sku_list.upc,
                                            edw_perfect_store_must_stock_sku_list.update_time,
                                            row_number() OVER (
                                                PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                                ) AS rn
                                        FROM edw_perfect_store_must_stock_sku_list
                                        ) derived_table12
                                    WHERE (derived_table12.rn = 1)
                                    ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                                )
                            ) msl ON (
                                (
                                    (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epsssd.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                    AND ((msl.yyyymm)::TEXT = (epsssd.yyyymm)::TEXT)
                                    )
                                )
                        )
                    WHERE (
                            (
                                (
                                    "substring" (
                                        (
                                            CASE 
                                                WHEN (
                                                        ((epsssd.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                                        OR (
                                                            (epsssd.yyyymm IS NULL)
                                                            AND ('' IS NULL)
                                                            )
                                                        )
                                                    THEN NULL::CHARACTER VARYING
                                                ELSE epsssd.yyyymm
                                                END
                                            )::TEXT,
                                        1,
                                        4
                                        )
                                    )::INTEGER
                                )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                            )
                    )
                ,                                     
union_6 as (
                
                
                
                SELECT 'Survey_Response' AS dataset,
                    epsssd.customer_code AS customerid,
                    NULL AS salespersonid,
                    NULL AS mrch_resp_startdt,
                    NULL AS mrch_resp_enddt,
                    NULL AS survey_enddate,
                    NULL AS questiontext,
                    (epsssd.display_yq)::CHARACTER VARYING AS value,
                    NULL AS mustcarryitem,
                    NULL AS presence,
                    NULL AS outofstock,
                    'Share of Shelf' AS kpi,
                    to_date(((epsssd.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                    NULL AS vst_status,
                    (
                        "substring" (
                            (epsssd.yyyymm)::TEXT,
                            1,
                            4
                            )
                        )::INTEGER AS fisc_yr,
                    (epsssd.yyyymm)::INTEGER AS fisc_per,
                    epssh.rsm_name AS firstname,
                    '' AS lastname,
                    epssh.cus_name AS customername,
                    'China' AS country,
                    epssh.region AS STATE,
                    epssh.ka_name AS storereference,
                    epssh.store_type AS storetype,
                    'MT' AS channel,
                    epssh.distributor AS salesgroup,
                    epssh.bu,
                    NULL AS prod_hier_l1,
                    NULL AS prod_hier_l4,
                    NULL AS prod_hier_l5,
                    NULL AS prod_hier_l6,
                    NULL AS prod_hier_l9,
                    NULL AS productname,
                    NULL AS eannumber,
                    (upper((epsssd.brand)::TEXT))::CHARACTER VARYING AS category,
                    NULL AS segment,
                    icpw.weight AS kpi_chnl_wt,
                    NULL AS actual,
                    NULL AS target,
                    1 AS mkt_share,
                    'denominator' AS ques_desc,
                    NULL AS "y/n_flag",
                    epssh.province,
                    epssh.city,
                    epssh.district,
                    epssh.address,
                    epssh.area,
                    epssh.ka_type,
                    epssh.store_property,
                    epssh.store_g_d,
                    epssh.supply_type,
                    epssh.agent,
                    epssh.customer_manager,
                    epssh.sales_supervisor,
                    epssh.asm_name
                FROM (
                    (
                        (
                            (
                                SELECT edw_perfect_store_source_sos_display.customer_code,
                                    edw_perfect_store_source_sos_display.unit_code,
                                    edw_perfect_store_source_sos_display.visit_customer_name,
                                    edw_perfect_store_source_sos_display.re,
                                    edw_perfect_store_source_sos_display.yyyymm,
                                    edw_perfect_store_source_sos_display.brand,
                                    edw_perfect_store_source_sos_display.display_yq,
                                    edw_perfect_store_source_sos_display.display_fk,
                                    edw_perfect_store_source_sos_display.update_time
                                FROM edw_perfect_store_source_sos_display
                                WHERE (
                                        (
                                            (edw_perfect_store_source_sos_display.display_yq IS NOT NULL)
                                            AND (edw_perfect_store_source_sos_display.display_fk IS NOT NULL)
                                            )
                                        AND (edw_perfect_store_source_sos_display.display_yq <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                        )
                                ) epsssd LEFT JOIN (
                                SELECT DISTINCT derived_table13.yyyymm,
                                    derived_table13.cus_code,
                                    derived_table13.unit_code,
                                    derived_table13.cus_name,
                                    derived_table13.province,
                                    derived_table13.city,
                                    derived_table13.district,
                                    derived_table13.address,
                                    derived_table13.bu,
                                    derived_table13.region,
                                    derived_table13.area,
                                    derived_table13.store_type,
                                    derived_table13.ka_name,
                                    derived_table13.ka_type,
                                    derived_table13.store_property,
                                    derived_table13.store_g_d,
                                    derived_table13.supply_type,
                                    derived_table13.agent,
                                    derived_table13.distributor,
                                    derived_table13.customer_manager,
                                    derived_table13.sales_supervisor,
                                    derived_table13.asm_name,
                                    derived_table13.rsm_name,
                                    derived_table13.update_time
                                FROM (
                                    SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                        edw_perfect_store_store_hierarchy.cus_code,
                                        edw_perfect_store_store_hierarchy.unit_code,
                                        edw_perfect_store_store_hierarchy.cus_name,
                                        edw_perfect_store_store_hierarchy.province,
                                        edw_perfect_store_store_hierarchy.city,
                                        edw_perfect_store_store_hierarchy.district,
                                        edw_perfect_store_store_hierarchy.address,
                                        edw_perfect_store_store_hierarchy.bu,
                                        edw_perfect_store_store_hierarchy.region,
                                        edw_perfect_store_store_hierarchy.area,
                                        edw_perfect_store_store_hierarchy.store_type,
                                        edw_perfect_store_store_hierarchy.ka_name,
                                        edw_perfect_store_store_hierarchy.ka_type,
                                        edw_perfect_store_store_hierarchy.store_property,
                                        edw_perfect_store_store_hierarchy.store_g_d,
                                        edw_perfect_store_store_hierarchy.supply_type,
                                        edw_perfect_store_store_hierarchy.agent,
                                        edw_perfect_store_store_hierarchy.distributor,
                                        edw_perfect_store_store_hierarchy.customer_manager,
                                        edw_perfect_store_store_hierarchy.sales_supervisor,
                                        edw_perfect_store_store_hierarchy.asm_name,
                                        edw_perfect_store_store_hierarchy.rsm_name,
                                        edw_perfect_store_store_hierarchy.update_time,
                                        row_number() OVER (
                                            PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                            edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                            ) AS row_num
                                    FROM edw_perfect_store_store_hierarchy
                                    ) derived_table13
                                WHERE (derived_table13.row_num = 1)
                                ) epssh ON (
                                    (
                                        (ltrim((epsssd.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                        AND ((epsssd.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                        )
                                    )
                            ) LEFT JOIN (
                            SELECT edw_vw_ps_weights.retail_environment AS re,
                                edw_vw_ps_weights.weight
                            FROM edw_vw_ps_weights
                            WHERE (
                                    (
                                        (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                        AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                        )
                                    AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                    )
                            ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                        ) JOIN (
                        SELECT DISTINCT epssm.yyyymm,
                            epssm.sfa_store_code
                        FROM (
                            (
                                SELECT edw_perfect_store_source_msl.sfa_store_code,
                                    edw_perfect_store_source_msl.j_code,
                                    edw_perfect_store_source_msl.re,
                                    edw_perfect_store_source_msl.yyyymm,
                                    edw_perfect_store_source_msl.brand,
                                    edw_perfect_store_source_msl.sku,
                                    edw_perfect_store_source_msl.PLAN,
                                    edw_perfect_store_source_msl.actual,
                                    edw_perfect_store_source_msl.update_time
                                FROM edw_perfect_store_source_msl
                                WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                ) epssm JOIN (
                                SELECT DISTINCT derived_table14.sku_name,
                                    derived_table14.sku_code,
                                    derived_table14.upc
                                FROM (
                                    SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                        edw_perfect_store_must_stock_sku_list.sku_code,
                                        edw_perfect_store_must_stock_sku_list.upc,
                                        edw_perfect_store_must_stock_sku_list.update_time,
                                        row_number() OVER (
                                            PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                            ) AS rn
                                    FROM edw_perfect_store_must_stock_sku_list
                                    ) derived_table14
                                WHERE (derived_table14.rn = 1)
                                ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                            )
                        ) msl ON (
                            (
                                (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epsssd.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                AND ((msl.yyyymm)::TEXT = (epsssd.yyyymm)::TEXT)
                                )
                            )
                    )
                WHERE (
                        (
                            (
                                "substring" (
                                    (
                                        CASE 
                                            WHEN (
                                                    ((epsssd.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                                    OR (
                                                        (epsssd.yyyymm IS NULL)
                                                        AND ('' IS NULL)
                                                        )
                                                    )
                                                THEN NULL::CHARACTER VARYING
                                            ELSE epsssd.yyyymm
                                            END
                                        )::TEXT,
                                    1,
                                    4
                                    )
                                )::INTEGER
                            )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                        )
                )
            ,
union_7 as  (
            
            
            SELECT 'Survey_Response' AS dataset,
                epssss.customer_code AS customerid,
                NULL AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                (epssss.brand_num)::CHARACTER VARYING AS value,
                NULL AS mustcarryitem,
                NULL AS presence,
                NULL AS outofstock,
                'Share of Shelf' AS kpi,
                to_date(((epssss.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
                NULL AS vst_status,
                (
                    "substring" (
                        (epssss.yyyymm)::TEXT,
                        1,
                        4
                        )
                    )::INTEGER AS fisc_yr,
                (epssss.yyyymm)::INTEGER AS fisc_per,
                epssh.rsm_name AS firstname,
                '' AS lastname,
                epssh.cus_name AS customername,
                'China' AS country,
                epssh.region AS STATE,
                epssh.ka_name AS storereference,
                epssh.store_type AS storetype,
                'MT' AS channel,
                epssh.distributor AS salesgroup,
                epssh.bu,
                NULL AS prod_hier_l1,
                NULL AS prod_hier_l4,
                NULL AS prod_hier_l5,
                NULL AS prod_hier_l6,
                NULL AS prod_hier_l9,
                NULL AS productname,
                NULL AS eannumber,
                (upper((epssss.brand)::TEXT))::CHARACTER VARYING AS category,
                NULL AS segment,
                icpw.weight AS kpi_chnl_wt,
                NULL AS actual,
                NULL AS target,
                epssss.shelf_value AS mkt_share,
                'numerator' AS ques_desc,
                NULL AS "y/n_flag",
                epssh.province,
                epssh.city,
                epssh.district,
                epssh.address,
                epssh.area,
                epssh.ka_type,
                epssh.store_property,
                epssh.store_g_d,
                epssh.supply_type,
                epssh.agent,
                epssh.customer_manager,
                epssh.sales_supervisor,
                epssh.asm_name
            FROM (
                (
                    (
                        (
                            SELECT edw_perfect_store_source_sos_shelf.customer_code,
                                edw_perfect_store_source_sos_shelf.unit_code,
                                edw_perfect_store_source_sos_shelf.visit_customer_name,
                                edw_perfect_store_source_sos_shelf.re,
                                edw_perfect_store_source_sos_shelf.yyyymm,
                                edw_perfect_store_source_sos_shelf.brand,
                                edw_perfect_store_source_sos_shelf.brand_num,
                                edw_perfect_store_source_sos_shelf.type_num,
                                edw_perfect_store_source_sos_shelf.shelf_value,
                                edw_perfect_store_source_sos_shelf.update_time
                            FROM edw_perfect_store_source_sos_shelf
                            WHERE (
                                    (
                                        (edw_perfect_store_source_sos_shelf.type_num IS NOT NULL)
                                        AND (edw_perfect_store_source_sos_shelf.brand_num IS NOT NULL)
                                        )
                                    AND (edw_perfect_store_source_sos_shelf.type_num <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                    )
                            ) epssss LEFT JOIN (
                            SELECT DISTINCT derived_table15.yyyymm,
                                derived_table15.cus_code,
                                derived_table15.unit_code,
                                derived_table15.cus_name,
                                derived_table15.province,
                                derived_table15.city,
                                derived_table15.district,
                                derived_table15.address,
                                derived_table15.bu,
                                derived_table15.region,
                                derived_table15.area,
                                derived_table15.store_type,
                                derived_table15.ka_name,
                                derived_table15.ka_type,
                                derived_table15.store_property,
                                derived_table15.store_g_d,
                                derived_table15.supply_type,
                                derived_table15.agent,
                                derived_table15.distributor,
                                derived_table15.customer_manager,
                                derived_table15.sales_supervisor,
                                derived_table15.asm_name,
                                derived_table15.rsm_name,
                                derived_table15.update_time
                            FROM (
                                SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                    edw_perfect_store_store_hierarchy.cus_code,
                                    edw_perfect_store_store_hierarchy.unit_code,
                                    edw_perfect_store_store_hierarchy.cus_name,
                                    edw_perfect_store_store_hierarchy.province,
                                    edw_perfect_store_store_hierarchy.city,
                                    edw_perfect_store_store_hierarchy.district,
                                    edw_perfect_store_store_hierarchy.address,
                                    edw_perfect_store_store_hierarchy.bu,
                                    edw_perfect_store_store_hierarchy.region,
                                    edw_perfect_store_store_hierarchy.area,
                                    edw_perfect_store_store_hierarchy.store_type,
                                    edw_perfect_store_store_hierarchy.ka_name,
                                    edw_perfect_store_store_hierarchy.ka_type,
                                    edw_perfect_store_store_hierarchy.store_property,
                                    edw_perfect_store_store_hierarchy.store_g_d,
                                    edw_perfect_store_store_hierarchy.supply_type,
                                    edw_perfect_store_store_hierarchy.agent,
                                    edw_perfect_store_store_hierarchy.distributor,
                                    edw_perfect_store_store_hierarchy.customer_manager,
                                    edw_perfect_store_store_hierarchy.sales_supervisor,
                                    edw_perfect_store_store_hierarchy.asm_name,
                                    edw_perfect_store_store_hierarchy.rsm_name,
                                    edw_perfect_store_store_hierarchy.update_time,
                                    row_number() OVER (
                                        PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                        edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                        ) AS row_num
                                FROM edw_perfect_store_store_hierarchy
                                ) derived_table15
                            WHERE (derived_table15.row_num = 1)
                            ) epssh ON (
                                (
                                    (ltrim((epssss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                    AND ((epssss.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                    )
                                )
                        ) LEFT JOIN (
                        SELECT edw_vw_ps_weights.retail_environment AS re,
                            edw_vw_ps_weights.weight
                        FROM edw_vw_ps_weights
                        WHERE (
                                (
                                    (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                    AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                    )
                                AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                                )
                        ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                    ) JOIN (
                    SELECT DISTINCT epssm.yyyymm,
                        epssm.sfa_store_code
                    FROM (
                        (
                            SELECT edw_perfect_store_source_msl.sfa_store_code,
                                edw_perfect_store_source_msl.j_code,
                                edw_perfect_store_source_msl.re,
                                edw_perfect_store_source_msl.yyyymm,
                                edw_perfect_store_source_msl.brand,
                                edw_perfect_store_source_msl.sku,
                                edw_perfect_store_source_msl.PLAN,
                                edw_perfect_store_source_msl.actual,
                                edw_perfect_store_source_msl.update_time
                            FROM edw_perfect_store_source_msl
                            WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                            ) epssm JOIN (
                            SELECT DISTINCT derived_table16.sku_name,
                                derived_table16.sku_code,
                                derived_table16.upc
                            FROM (
                                SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                    edw_perfect_store_must_stock_sku_list.sku_code,
                                    edw_perfect_store_must_stock_sku_list.upc,
                                    edw_perfect_store_must_stock_sku_list.update_time,
                                    row_number() OVER (
                                        PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                        ) AS rn
                                FROM edw_perfect_store_must_stock_sku_list
                                ) derived_table16
                            WHERE (derived_table16.rn = 1)
                            ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                        )
                    ) msl ON (
                        (
                            (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                            AND ((msl.yyyymm)::TEXT = (epssss.yyyymm)::TEXT)
                            )
                        )
                )
            WHERE (
                    (
                        (
                            "substring" (
                                (
                                    CASE 
                                        WHEN (
                                                ((epssss.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                                OR (
                                                    (epssss.yyyymm IS NULL)
                                                    AND ('' IS NULL)
                                                    )
                                                )
                                            THEN NULL::CHARACTER VARYING
                                        ELSE epssss.yyyymm
                                        END
                                    )::TEXT,
                                1,
                                4
                                )
                            )::INTEGER
                        )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                    )
            )
        ,
union_8 as (
       
        SELECT 'Survey_Response' AS dataset,
            epssss.customer_code AS customerid,
            NULL AS salespersonid,
            NULL AS mrch_resp_startdt,
            NULL AS mrch_resp_enddt,
            NULL AS survey_enddate,
            NULL AS questiontext,
            (epssss.type_num)::CHARACTER VARYING AS value,
            NULL AS mustcarryitem,
            NULL AS presence,
            NULL AS outofstock,
            'Share of Shelf' AS kpi,
            to_date(((epssss.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
            NULL AS vst_status,
            (
                "substring" (
                    (epssss.yyyymm)::TEXT,
                    1,
                    4
                    )
                )::INTEGER AS fisc_yr,
            (epssss.yyyymm)::INTEGER AS fisc_per,
            epssh.rsm_name AS firstname,
            '' AS lastname,
            epssh.cus_name AS customername,
            'China' AS country,
            epssh.region AS STATE,
            epssh.ka_name AS storereference,
            epssh.store_type AS storetype,
            'MT' AS channel,
            epssh.distributor AS salesgroup,
            epssh.bu,
            NULL AS prod_hier_l1,
            NULL AS prod_hier_l4,
            NULL AS prod_hier_l5,
            NULL AS prod_hier_l6,
            NULL AS prod_hier_l9,
            NULL AS productname,
            NULL AS eannumber,
            (upper((epssss.brand)::TEXT))::CHARACTER VARYING AS category,
            NULL AS segment,
            icpw.weight AS kpi_chnl_wt,
            NULL AS actual,
            NULL AS target,
            epssss.shelf_value AS mkt_share,
            'denominator' AS ques_desc,
            NULL AS "y/n_flag",
            epssh.province,
            epssh.city,
            epssh.district,
            epssh.address,
            epssh.area,
            epssh.ka_type,
            epssh.store_property,
            epssh.store_g_d,
            epssh.supply_type,
            epssh.agent,
            epssh.customer_manager,
            epssh.sales_supervisor,
            epssh.asm_name
        FROM (
            (
                (
                    (
                        SELECT edw_perfect_store_source_sos_shelf.customer_code,
                            edw_perfect_store_source_sos_shelf.unit_code,
                            edw_perfect_store_source_sos_shelf.visit_customer_name,
                            edw_perfect_store_source_sos_shelf.re,
                            edw_perfect_store_source_sos_shelf.yyyymm,
                            edw_perfect_store_source_sos_shelf.brand,
                            edw_perfect_store_source_sos_shelf.brand_num,
                            edw_perfect_store_source_sos_shelf.type_num,
                            edw_perfect_store_source_sos_shelf.shelf_value,
                            edw_perfect_store_source_sos_shelf.update_time
                        FROM edw_perfect_store_source_sos_shelf
                        WHERE (
                                (
                                    (edw_perfect_store_source_sos_shelf.type_num IS NOT NULL)
                                    AND (edw_perfect_store_source_sos_shelf.brand_num IS NOT NULL)
                                    )
                                AND (edw_perfect_store_source_sos_shelf.type_num <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                                )
                        ) epssss LEFT JOIN (
                        SELECT DISTINCT derived_table17.yyyymm,
                            derived_table17.cus_code,
                            derived_table17.unit_code,
                            derived_table17.cus_name,
                            derived_table17.province,
                            derived_table17.city,
                            derived_table17.district,
                            derived_table17.address,
                            derived_table17.bu,
                            derived_table17.region,
                            derived_table17.area,
                            derived_table17.store_type,
                            derived_table17.ka_name,
                            derived_table17.ka_type,
                            derived_table17.store_property,
                            derived_table17.store_g_d,
                            derived_table17.supply_type,
                            derived_table17.agent,
                            derived_table17.distributor,
                            derived_table17.customer_manager,
                            derived_table17.sales_supervisor,
                            derived_table17.asm_name,
                            derived_table17.rsm_name,
                            derived_table17.update_time
                        FROM (
                            SELECT edw_perfect_store_store_hierarchy.yyyymm,
                                edw_perfect_store_store_hierarchy.cus_code,
                                edw_perfect_store_store_hierarchy.unit_code,
                                edw_perfect_store_store_hierarchy.cus_name,
                                edw_perfect_store_store_hierarchy.province,
                                edw_perfect_store_store_hierarchy.city,
                                edw_perfect_store_store_hierarchy.district,
                                edw_perfect_store_store_hierarchy.address,
                                edw_perfect_store_store_hierarchy.bu,
                                edw_perfect_store_store_hierarchy.region,
                                edw_perfect_store_store_hierarchy.area,
                                edw_perfect_store_store_hierarchy.store_type,
                                edw_perfect_store_store_hierarchy.ka_name,
                                edw_perfect_store_store_hierarchy.ka_type,
                                edw_perfect_store_store_hierarchy.store_property,
                                edw_perfect_store_store_hierarchy.store_g_d,
                                edw_perfect_store_store_hierarchy.supply_type,
                                edw_perfect_store_store_hierarchy.agent,
                                edw_perfect_store_store_hierarchy.distributor,
                                edw_perfect_store_store_hierarchy.customer_manager,
                                edw_perfect_store_store_hierarchy.sales_supervisor,
                                edw_perfect_store_store_hierarchy.asm_name,
                                edw_perfect_store_store_hierarchy.rsm_name,
                                edw_perfect_store_store_hierarchy.update_time,
                                row_number() OVER (
                                    PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                                    edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                                    ) AS row_num
                            FROM edw_perfect_store_store_hierarchy
                            ) derived_table17
                        WHERE (derived_table17.row_num = 1)
                        ) epssh ON (
                            (
                                (ltrim((epssss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                                AND ((epssss.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    SELECT edw_vw_ps_weights.retail_environment AS re,
                        edw_vw_ps_weights.weight
                    FROM edw_vw_ps_weights
                    WHERE (
                            (
                                (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                                )
                            AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                            )
                    ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
                ) JOIN (
                SELECT DISTINCT epssm.yyyymm,
                    epssm.sfa_store_code
                FROM (
                    (
                        SELECT edw_perfect_store_source_msl.sfa_store_code,
                            edw_perfect_store_source_msl.j_code,
                            edw_perfect_store_source_msl.re,
                            edw_perfect_store_source_msl.yyyymm,
                            edw_perfect_store_source_msl.brand,
                            edw_perfect_store_source_msl.sku,
                            edw_perfect_store_source_msl.PLAN,
                            edw_perfect_store_source_msl.actual,
                            edw_perfect_store_source_msl.update_time
                        FROM edw_perfect_store_source_msl
                        WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                        ) epssm JOIN (
                        SELECT DISTINCT derived_table18.sku_name,
                            derived_table18.sku_code,
                            derived_table18.upc
                        FROM (
                            SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                                edw_perfect_store_must_stock_sku_list.sku_code,
                                edw_perfect_store_must_stock_sku_list.upc,
                                edw_perfect_store_must_stock_sku_list.update_time,
                                row_number() OVER (
                                    PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                                    ) AS rn
                            FROM edw_perfect_store_must_stock_sku_list
                            ) derived_table18
                        WHERE (derived_table18.rn = 1)
                        ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
                    )
                ) msl ON (
                    (
                        (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssss.customer_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                        AND ((msl.yyyymm)::TEXT = (epssss.yyyymm)::TEXT)
                        )
                    )
            )
        WHERE (
                (
                    (
                        "substring" (
                            (
                                CASE 
                                    WHEN (
                                            ((epssss.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                            OR (
                                                (epssss.yyyymm IS NULL)
                                                AND ('' IS NULL)
                                                )
                                            )
                                        THEN NULL::CHARACTER VARYING
                                    ELSE epssss.yyyymm
                                    END
                                )::TEXT,
                            1,
                            4
                            )
                        )::INTEGER
                    )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
                )
        ),

union_9 as (
SELECT 'Survey_Response' AS dataset,
    epssp.store_code AS customerid,
    NULL AS salespersonid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS survey_enddate,
    (epssp.ques_text)::CHARACTER VARYING AS questiontext,
    NULL AS value,
    NULL AS mustcarryitem,
    NULL AS presence,
    NULL AS outofstock,
    'Promo Compliance' AS kpi,
    to_date(((epssp.yyyymm)::TEXT || ('15'::CHARACTER VARYING)::TEXT), ('YYYYMMDD'::CHARACTER VARYING)::TEXT) AS scheduleddate,
    NULL AS vst_status,
    (
        "substring" (
            (epssp.yyyymm)::TEXT,
            1,
            4
            )
        )::INTEGER AS fisc_yr,
    (epssp.yyyymm)::INTEGER AS fisc_per,
    epssh.rsm_name AS firstname,
    '' AS lastname,
    epssh.cus_name AS customername,
    'China' AS country,
    epssh.region AS STATE,
    epssh.ka_name AS storereference,
    epssh.store_type AS storetype,
    'MT' AS channel,
    epssh.distributor AS salesgroup,
    epssh.bu,
    NULL AS prod_hier_l1,
    NULL AS prod_hier_l4,
    NULL AS prod_hier_l5,
    NULL AS prod_hier_l6,
    NULL AS prod_hier_l9,
    NULL AS productname,
    NULL AS eannumber,
    (upper((epssp.brand)::TEXT))::CHARACTER VARYING AS category,
    NULL AS segment,
    icpw.weight AS kpi_chnl_wt,
    NULL AS actual,
    NULL AS target,
    0 AS mkt_share,
    NULL AS ques_desc,
    CASE 
        WHEN (epssp.achieved = 1)
            THEN 'YES'::CHARACTER VARYING
        ELSE 'NO'::CHARACTER VARYING
        END AS "y/n_flag",
    epssh.province,
    epssh.city,
    epssh.district,
    epssh.address,
    epssh.area,
    epssh.ka_type,
    epssh.store_property,
    epssh.store_g_d,
    epssh.supply_type,
    epssh.agent,
    epssh.customer_manager,
    epssh.sales_supervisor,
    epssh.asm_name
FROM (
    (
        (
            (
                SELECT trans.store_code,
                    trans.j_code,
                    trans.re,
                    trans.yyyymm,
                    trans.brand,
                    trans.kpis_plan,
                    trans.kpis_ach,
                    1 AS PLAN,
                    CASE 
                        WHEN ((((rowref.rownum)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4) <= trans.kpis_ach)
                            THEN 1
                        ELSE 0
                        END AS achieved,
                    trans.update_time,
                    (('Plan - '::CHARACTER VARYING)::TEXT || ((rowref.rownum)::CHARACTER VARYING)::TEXT) AS ques_text
                FROM (
                    edw_perfect_store_source_prom trans JOIN (
                        SELECT row_number() OVER (ORDER BY 1) AS rownum
                        FROM edw_perfect_store_source_prom
                        ) rowref ON ((trans.kpis_plan >= (((rowref.rownum)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4)))
                    )
                WHERE (
                        (
                            (trans.kpis_plan IS NOT NULL)
                            AND (trans.kpis_plan <> (((0)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                            )
                        AND (trans.kpis_ach IS NOT NULL)
                        )
                ORDER BY trans.yyyymm,
                    trans.store_code,
                    trans.re,
                    trans.brand
                ) epssp LEFT JOIN (
                SELECT DISTINCT derived_table19.yyyymm,
                    derived_table19.cus_code,
                    derived_table19.unit_code,
                    derived_table19.cus_name,
                    derived_table19.province,
                    derived_table19.city,
                    derived_table19.district,
                    derived_table19.address,
                    derived_table19.bu,
                    derived_table19.region,
                    derived_table19.area,
                    derived_table19.store_type,
                    derived_table19.ka_name,
                    derived_table19.ka_type,
                    derived_table19.store_property,
                    derived_table19.store_g_d,
                    derived_table19.supply_type,
                    derived_table19.agent,
                    derived_table19.distributor,
                    derived_table19.customer_manager,
                    derived_table19.sales_supervisor,
                    derived_table19.asm_name,
                    derived_table19.rsm_name,
                    derived_table19.update_time
                FROM (
                    SELECT edw_perfect_store_store_hierarchy.yyyymm,
                        edw_perfect_store_store_hierarchy.cus_code,
                        edw_perfect_store_store_hierarchy.unit_code,
                        edw_perfect_store_store_hierarchy.cus_name,
                        edw_perfect_store_store_hierarchy.province,
                        edw_perfect_store_store_hierarchy.city,
                        edw_perfect_store_store_hierarchy.district,
                        edw_perfect_store_store_hierarchy.address,
                        edw_perfect_store_store_hierarchy.bu,
                        edw_perfect_store_store_hierarchy.region,
                        edw_perfect_store_store_hierarchy.area,
                        edw_perfect_store_store_hierarchy.store_type,
                        edw_perfect_store_store_hierarchy.ka_name,
                        edw_perfect_store_store_hierarchy.ka_type,
                        edw_perfect_store_store_hierarchy.store_property,
                        edw_perfect_store_store_hierarchy.store_g_d,
                        edw_perfect_store_store_hierarchy.supply_type,
                        edw_perfect_store_store_hierarchy.agent,
                        edw_perfect_store_store_hierarchy.distributor,
                        edw_perfect_store_store_hierarchy.customer_manager,
                        edw_perfect_store_store_hierarchy.sales_supervisor,
                        edw_perfect_store_store_hierarchy.asm_name,
                        edw_perfect_store_store_hierarchy.rsm_name,
                        edw_perfect_store_store_hierarchy.update_time,
                        row_number() OVER (
                            PARTITION BY edw_perfect_store_store_hierarchy.yyyymm,
                            edw_perfect_store_store_hierarchy.cus_code ORDER BY edw_perfect_store_store_hierarchy.update_time DESC
                            ) AS row_num
                    FROM edw_perfect_store_store_hierarchy
                    ) derived_table19
                WHERE (derived_table19.row_num = 1)
                ) epssh ON (
                    (
                        (ltrim((epssp.store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssh.cus_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                        AND ((epssp.yyyymm)::TEXT = (epssh.yyyymm)::TEXT)
                        )
                    )
            ) LEFT JOIN (
            SELECT edw_vw_ps_weights.retail_environment AS re,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE (
                    (
                        (upper((edw_vw_ps_weights.kpi)::TEXT) = ('PROMO COMPLIANCE'::CHARACTER VARYING)::TEXT)
                        AND (upper((edw_vw_ps_weights.channel)::TEXT) = ('MT'::CHARACTER VARYING)::TEXT)
                        )
                    AND (upper((edw_vw_ps_weights.market)::TEXT) = ('CHINA'::CHARACTER VARYING)::TEXT)
                    )
            ) icpw ON ((upper((epssh.store_type)::TEXT) = upper((icpw.re)::TEXT)))
        ) JOIN (
        SELECT DISTINCT epssm.yyyymm,
            epssm.sfa_store_code
        FROM (
            (
                SELECT edw_perfect_store_source_msl.sfa_store_code,
                    edw_perfect_store_source_msl.j_code,
                    edw_perfect_store_source_msl.re,
                    edw_perfect_store_source_msl.yyyymm,
                    edw_perfect_store_source_msl.brand,
                    edw_perfect_store_source_msl.sku,
                    edw_perfect_store_source_msl.PLAN,
                    edw_perfect_store_source_msl.actual,
                    edw_perfect_store_source_msl.update_time
                FROM edw_perfect_store_source_msl
                WHERE (edw_perfect_store_source_msl.PLAN = (((1)::NUMERIC)::NUMERIC(18, 0))::NUMERIC(20, 4))
                ) epssm JOIN (
                SELECT DISTINCT derived_table20.sku_name,
                    derived_table20.sku_code,
                    derived_table20.upc
                FROM (
                    SELECT edw_perfect_store_must_stock_sku_list.sku_name,
                        edw_perfect_store_must_stock_sku_list.sku_code,
                        edw_perfect_store_must_stock_sku_list.upc,
                        edw_perfect_store_must_stock_sku_list.update_time,
                        row_number() OVER (
                            PARTITION BY edw_perfect_store_must_stock_sku_list.sku_name ORDER BY edw_perfect_store_must_stock_sku_list.sku_name DESC NULLS LAST
                            ) AS rn
                    FROM edw_perfect_store_must_stock_sku_list
                    ) derived_table20
                WHERE (derived_table20.rn = 1)
                ) epsmssl ON ((trim((epsmssl.sku_name)::TEXT) = trim((epssm.sku)::TEXT)))
            )
        ) msl ON (
            (
                (ltrim((msl.sfa_store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = ltrim((epssp.store_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
                AND ((msl.yyyymm)::TEXT = (epssp.yyyymm)::TEXT)
                )
            )
    )
WHERE (
        (
            (
                "substring" (
                    (
                        CASE 
                            WHEN (
                                    ((epssp.yyyymm)::TEXT = (''::CHARACTER VARYING)::TEXT)
                                    OR (
                                        (epssp.yyyymm IS NULL)
                                        AND ('' IS NULL)
                                        )
                                    )
                                THEN NULL::CHARACTER VARYING
                            ELSE epssp.yyyymm
                            END
                        )::TEXT,
                    1,
                    4
                    )
                )::INTEGER
            )::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2
        )
),
merge_all as
(
    select * from union_1
    UNION ALL
    select * from union_2
    UNION ALL
    select * from union_3
    UNION ALL
    select * from union_4
    UNION ALL
    select * from union_5
    UNION ALL
    select * from union_6
    UNION ALL
    select * from union_7
     UNION ALL
    select * from union_8
    UNION ALL
    select * from union_9
),
final as
(
    select
    dataset::varchar(22) as dataset,
	customerid::varchar(200) as customerid,
	salespersonid::varchar(16777216) as salespersonid,
	mrch_resp_startdt::varchar(16777216) as mrch_resp_startdt,
	mrch_resp_enddt::varchar(16777216) as mrch_resp_enddt,
	survey_enddate::varchar(16777216) as survey_enddate,
	questiontext::varchar(28) as questiontext,
	value::varchar(40) as value,
	mustcarryitem::varchar(4) as mustcarryitem,
	presence::varchar(5) as presence,
	outofstock::varchar(4) as outofstock,
	kpi::varchar(20) as kpi,
	scheduleddate::date as scheduleddate,
	vst_status::varchar(9) as vst_status, 
	fisc_yr::number(38,0) as fisc_yr,
	fisc_per::number(38,0) as fisc_per,
	firstname::varchar(100) as firstname,
	lastname::varchar(16777216) as lastname,
	customername::varchar(500) as customername,
	country::varchar(5) as country,
	state::varchar(100) as state,
	storereference::varchar(100) as storereference,
	storetype::varchar(100) as storetype,
	channel::varchar(2) as channel,
	salesgroup::varchar(100) as salesgroup,
	bu::varchar(100) as bu,
	prod_hier_l1::varchar(5) as prod_hier_l1,
	prod_hier_l4::varchar(30) as prod_hier_l4,
	prod_hier_l5::varchar(300) as prod_hier_l5,
	prod_hier_l6::varchar(200) as prod_hier_l6,
	prod_hier_l9::varchar(100) as prod_hier_l9,
	productname::varchar(100) as productname,
	eannumber::varchar(100) as eannumber,
	category::varchar(300) as category,
	segment::varchar(16777216) as segment,
	kpi_chnl_wt::number(38,5) as kpi_chnl_wt,
	actual::varchar(40) as actual,
	TARGET::VARCHAR(40) AS TARGET,
	mkt_share::number(20,4) as mkt_share,
	ques_desc::varchar(11) as ques_desc,
	"y/n_flag"::VARCHAR(3) as "y/n_flag",
	province::varchar(100) as province,
	city::varchar(100) as city,
	district::varchar(100) as district,
	address::varchar(500) as address,
	area::varchar(100) as area,
	ka_type::varchar(100) as ka_type,
	store_property::varchar(100) as store_property,
	store_g_d::varchar(100) as store_g_d,
	supply_type::varchar(100) as supply_type,
	agent::varchar(100) as agent,
	customer_manager::varchar(100) as customer_manager,
	sales_supervisor::varchar(100) as sales_supervisor,
	asm_name::varchar(100) as asm_name
    from merge_all
)
select * from final