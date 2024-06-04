with edw_tw_bu_forecast_prod_hier as (
    select * from {{ ref('ntaedw_integration__edw_tw_bu_forecast_prod_hier') }}
),
edw_tw_bu_forecast_sku as (
    select * from {{ ref('ntaedw_integration__edw_tw_bu_forecast_sku') }}   
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_product_attr_dim as (
    select * from aspedw_integration.edw_product_attr_dim
),
edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_customer_attr_hier_dim as (
    select * from {{ ref('aspedw_integration__edw_customer_attr_hier_dim') }}
),
edw_customer_attr_flat_dim as (
    select * from aspedw_integration.edw_customer_attr_flat_dim
),
v_intrm_crncy_exch as (
    select * from {{ ref('ntaedw_integration__v_intrm_crncy_exch') }} 
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
itg_query_parameters as (
    select * from {{ source('ntaitg_integration', 'itg_query_parameters') }}
),
edw_tw_bp_target as (
    select * from {{ ref('ntaedw_integration__edw_tw_bp_target') }}
),
bu_forecast as 
(
    SELECT 
        'BU_FORECAST' AS subsource_type,
        'TW' AS cntry_cd,
        derivedtab2.crncy_cd,
        derivedtab2.bu_version,
        (derivedtab2.forecast_on_year)::integer AS forecast_on_year,
        (derivedtab2.forecast_on_month)::integer AS forecast_on_month,
        derivedtab2.forecast_for_year,
        derivedtab2.forecast_for_mnth,
        null as caln_day,
        (to_char(current_date,('YYYYMMDD'::character varying)::text))::character varying AS latest_date,
        (
            (
                ((calendar.fisc_yr)::character varying)::text || 
                "substring"(((calendar.fisc_per)::character varying)::text,6
                )
            )
        )::character varying AS latest_fisc_yrmnth,
        derivedtab2.sls_grp,
        derivedtab2.channel,
        derivedtab2.sls_ofc,
        derivedtab2.sls_ofc_desc,
        derivedtab2.strategy_customer_hierachy_name,
        (derivedtab2.prod_hier_l8)::character varying AS lph_level_6,
        null as gts_bp_tgt,
        (
            sum(bfph.pre_sales_before_returns) - COALESCE(sum(bfph.sr), (0)::double precision)
        ) AS gts_bu_forecast,
        null as gts,
        null as trading_term_act,
        null as hidden_discnt_act,
        null as rtn_act,
        0.0 AS gts_act,
        0.0 AS nts_bp_tgt,
        (
            (
                (
                    (
                        (
                            sum(bfph.pre_sales_before_returns) - COALESCE(sum(bfph.price_off), (0)::double precision)
                        ) - COALESCE(sum(bfph.display), (0)::double precision)
                    ) - COALESCE(sum(bfph.dm), (0)::double precision)
                ) - COALESCE(sum(bfph.other_support), (0)::double precision)
            ) - COALESCE(sum(bfph.sr), (0)::double precision)
        ) AS nts_bu_forecast,
        0.0 AS nts_act,
        0.0 AS tp_bp_tgt,
        (
            (
                (
                    sum(COALESCE(bfph.display, (0)::double precision)) + sum(COALESCE(bfph.dm, (0)::double precision))
                ) + sum(
                    COALESCE(bfph.other_support, (0)::double precision)
                )
            ) + COALESCE(sum(bfph.price_off), (0)::double precision)
        ) AS tp_bu_forecast,
        0.0 AS tp_act,
        sum(bfph.price_off) AS price_off_bu_forecast,
        sum(bfph.display) AS display_bu_forecast,
        sum(bfph.dm) AS dm_bu_forecast,
        sum(bfph.other_support) AS other_support_bu_forecast,
        sum(bfph.sr) AS sr_bu_forecast,
        (derivedtab2.prod_hier_l4)::character varying AS brand,
        crncy.to_crncy,
        crncy.ex_rt
    FROM 
        (
            (
                (
                    (
                        SELECT 'TWD'::character varying AS crncy_cd,
                            bfs.bu_version,
                            bfs.forecast_on_year,
                            bfs.forecast_on_month,
                            bfs.forecast_for_year,
                            bfs.forecast_for_mnth,
                            bfs.sls_grp,
                            bfs.channel,
                            bfs.sls_ofc,
                            bfs.sls_ofc_desc,
                            prod_hier.prod_hier_l8,
                            prod_hier.prod_hier_l4,
                            bfs.strategy_customer_hierachy_name,
                            sum(bfs.system_list_price) AS system_list_price,
                            sum(bfs.gross_invoice_price) AS gross_invoice_price,
                            sum(bfs.gross_invoice_price_lesst_terms) AS gross_invoice_price_lesst_terms,
                            sum(bfs.rf_sellout_qty) AS rf_sellout_qty,
                            sum(bfs.rf_sellin_qty) AS rf_sellin_qty,
                            sum(bfs.price_off) AS price_off,
                            sum(bfs.pre_sales_before_returns) AS pre_sales_before_returns
                        FROM 
                            (
                                (
                                    SELECT a.bu_version,
                                        a.forecast_on_year,
                                        a.forecast_on_month,
                                        a.forecast_for_year,
                                        a.forecast_for_mnth,
                                        a.sls_grp,
                                        a.channel,
                                        a.sls_ofc,
                                        a.sls_ofc_desc,
                                        a.sap_code,
                                        a.strategy_customer_hierachy_name,
                                        a.system_list_price,
                                        a.gross_invoice_price,
                                        a.gross_invoice_price_lesst_terms,
                                        a.rf_sellout_qty,
                                        a.rf_sellin_qty,
                                        a.price_off,
                                        a.pre_sales_before_returns,
                                        a.load_date
                                    FROM 
                                        (
                                            (
                                                SELECT edw_tw_bu_forecast_sku.bu_version,
                                                    edw_tw_bu_forecast_sku.forecast_on_year,
                                                    edw_tw_bu_forecast_sku.forecast_on_month,
                                                    (edw_tw_bu_forecast_sku.forecast_for_year)::integer AS forecast_for_year,
                                                    CASE
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Jan'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Jan' IS NULL)
                                                            )
                                                        ) THEN 1
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Feb'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Feb' IS NULL)
                                                            )
                                                        ) THEN 2
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Mar'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Mar' IS NULL)
                                                            )
                                                        ) THEN 3
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Apr'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Apr' IS NULL)
                                                            )
                                                        ) THEN 4
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('May'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('May' IS NULL)
                                                            )
                                                        ) THEN 5
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Jun'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Jun' IS NULL)
                                                            )
                                                        ) THEN 6
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Jul'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Jul' IS NULL)
                                                            )
                                                        ) THEN 7
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Aug'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Aug' IS NULL)
                                                            )
                                                        ) THEN 8
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Sep'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Sep' IS NULL)
                                                            )
                                                        ) THEN 9
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Oct'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Oct' IS NULL)
                                                            )
                                                        ) THEN 10
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Nov'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Nov' IS NULL)
                                                            )
                                                        ) THEN 11
                                                        WHEN (
                                                            (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth)::text = ('Dec'::character varying)::text
                                                            )
                                                            OR (
                                                                (edw_tw_bu_forecast_sku.forecast_for_mnth IS NULL)
                                                                AND ('Dec' IS NULL)
                                                            )
                                                        ) THEN 12
                                                        ELSE NULL::integer
                                                    END AS forecast_for_mnth,
                                                    edw_tw_bu_forecast_sku.sls_grp,
                                                    edw_tw_bu_forecast_sku.channel,
                                                    edw_tw_bu_forecast_sku.sls_ofc,
                                                    edw_tw_bu_forecast_sku.sls_ofc_desc,
                                                    edw_tw_bu_forecast_sku.sap_code,
                                                    edw_tw_bu_forecast_sku.strategy_customer_hierachy_name,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.system_list_price,
                                                        (0)::double precision
                                                    ) AS system_list_price,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.gross_invoice_price,
                                                        (0)::double precision
                                                    ) AS gross_invoice_price,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.gross_invoice_price_lesst_terms,
                                                        (0)::double precision
                                                    ) AS gross_invoice_price_lesst_terms,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.rf_sellout_qty,
                                                        (0)::double precision
                                                    ) AS rf_sellout_qty,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.rf_sellin_qty,
                                                        (0)::double precision
                                                    ) AS rf_sellin_qty,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.price_off,
                                                        (0)::double precision
                                                    ) AS price_off,
                                                    COALESCE(
                                                        edw_tw_bu_forecast_sku.pre_sales_before_returns,
                                                        (0)::double precision
                                                    ) AS pre_sales_before_returns,
                                                    edw_tw_bu_forecast_sku.load_date
                                                FROM edw_tw_bu_forecast_sku
                                            ) a
                                            JOIN 
                                            (
                                                SELECT edw_tw_bu_forecast_prod_hier.forecast_on_year,
                                                    "max"(
                                                        (
                                                            (edw_tw_bu_forecast_prod_hier.forecast_on_year)::text || (edw_tw_bu_forecast_prod_hier.forecast_on_month)::text
                                                        )
                                                    ) AS forecast_on_period
                                                FROM edw_tw_bu_forecast_prod_hier
                                                GROUP BY edw_tw_bu_forecast_prod_hier.forecast_on_year
                                            ) b ON 
                                            (
                                                (
                                                    (
                                                        (a.forecast_on_year)::text = (b.forecast_on_year)::text
                                                    )
                                                    AND (
                                                        (
                                                            (a.forecast_on_year)::text || (a.forecast_on_month)::text
                                                        ) = b.forecast_on_period
                                                    )
                                                )
                                            )
                                        )
                                ) bfs
                                LEFT JOIN 
                                (
                                    SELECT mat.sap_code,
                                        "max"((prod_hier.prod_hier_l8)::text) AS prod_hier_l8,
                                        "max"((prod_hier.prod_hier_l4)::text) AS prod_hier_l4
                                    FROM 
                                        (
                                            (
                                                SELECT bu.sap_code,
                                                    mat_dim.ean_num AS ean
                                                FROM 
                                                    (
                                                        (
                                                            SELECT DISTINCT edw_tw_bu_forecast_sku.sap_code FROM edw_tw_bu_forecast_sku
                                                        ) bu
                                                        LEFT JOIN 
                                                        (
                                                            SELECT edw_material_sales_dim.sls_org,
                                                                edw_material_sales_dim.dstr_chnl,
                                                                edw_material_sales_dim.matl_num,
                                                                edw_material_sales_dim.base_unit,
                                                                edw_material_sales_dim.matl_grp_1,
                                                                edw_material_sales_dim.prod_hierarchy,
                                                                edw_material_sales_dim.commsn_grp,
                                                                edw_material_sales_dim.vol_rebt_grp,
                                                                edw_material_sales_dim.pharma_cent_no,
                                                                edw_material_sales_dim.del_fl,
                                                                edw_material_sales_dim.matl_grp_2,
                                                                edw_material_sales_dim.matl_grp_3,
                                                                edw_material_sales_dim.matl_grp_4,
                                                                edw_material_sales_dim.matl_grp_5,
                                                                edw_material_sales_dim.matl_stats_grp,
                                                                edw_material_sales_dim.asrtmnt_grade,
                                                                edw_material_sales_dim.afs_vas_matl_grp,
                                                                edw_material_sales_dim.afs_prc_in,
                                                                edw_material_sales_dim.predecessor,
                                                                edw_material_sales_dim.sku_id,
                                                                edw_material_sales_dim.prodt_alloc_det_proc,
                                                                edw_material_sales_dim.num_pcs_in,
                                                                edw_material_sales_dim.ean_num,
                                                                edw_material_sales_dim.old_matl_num,
                                                                edw_material_sales_dim.delv_plnt,
                                                                edw_material_sales_dim.cash_disc_ind,
                                                                edw_material_sales_dim.prc_grp_matl,
                                                                edw_material_sales_dim.acct_asgn_grp,
                                                                edw_material_sales_dim.itm_cat_grp,
                                                                edw_material_sales_dim.min_ordr_qty,
                                                                edw_material_sales_dim.min_delv_qty,
                                                                edw_material_sales_dim.delv_unit,
                                                                edw_material_sales_dim.delv_uom,
                                                                edw_material_sales_dim.sls_unit,
                                                                edw_material_sales_dim.launch_dt,
                                                                edw_material_sales_dim.npi_in,
                                                                edw_material_sales_dim.lcl_matl_grp_1,
                                                                edw_material_sales_dim.lcl_matl_grp_2,
                                                                edw_material_sales_dim.lcl_matl_grp_3,
                                                                edw_material_sales_dim.lcl_matl_grp_4,
                                                                edw_material_sales_dim.lcl_matl_grp_5,
                                                                edw_material_sales_dim.lcl_matl_grp_6,
                                                                edw_material_sales_dim.npi_in_apo,
                                                                edw_material_sales_dim.copy_hist,
                                                                edw_material_sales_dim.prod_classftn,
                                                                edw_material_sales_dim.fcst_indc_apo,
                                                                edw_material_sales_dim.prod_type_apo,
                                                                edw_material_sales_dim.mstr_cd,
                                                                edw_material_sales_dim.med_desc,
                                                                edw_material_sales_dim.crt_dttm,
                                                                edw_material_sales_dim.updt_dttm
                                                            FROM edw_material_sales_dim
                                                            WHERE (
                                                                    (
                                                                        (edw_material_sales_dim.sls_org)::text = ('1200'::character varying)::text
                                                                    )
                                                                    OR (
                                                                        (edw_material_sales_dim.sls_org)::text = ('120S'::character varying)::text
                                                                    )
                                                                )
                                                        ) mat_dim ON 
                                                        (
                                                            (
                                                                (bu.sap_code)::text = ltrim(
                                                                    (mat_dim.matl_num)::text,
                                                                    ('0'::character varying)::text
                                                                )
                                                            )
                                                        )
                                                    )
                                                ORDER BY bu.sap_code
                                            ) mat
                                            LEFT JOIN 
                                            (
                                                SELECT DISTINCT edw_product_attr_dim.ean,
                                                    edw_product_attr_dim.prod_hier_l4,
                                                    edw_product_attr_dim.prod_hier_l8
                                                FROM edw_product_attr_dim
                                                WHERE (
                                                        (edw_product_attr_dim.cntry)::text = ('TW'::character varying)::text
                                                    )
                                            ) prod_hier ON (((mat.ean)::text = (prod_hier.ean)::text))
                                        )
                                    GROUP BY mat.sap_code
                                    ORDER BY mat.sap_code
                                ) prod_hier ON (
                                    (
                                        (bfs.sap_code)::text = (prod_hier.sap_code)::text
                                    )
                                )
                            )
                        GROUP BY bfs.bu_version,
                            bfs.forecast_on_year,
                            bfs.forecast_on_month,
                            bfs.forecast_for_year,
                            bfs.forecast_for_mnth,
                            bfs.sls_ofc,
                            bfs.sls_ofc_desc,
                            bfs.sls_grp,
                            bfs.channel,
                            prod_hier.prod_hier_l8,
                            prod_hier.prod_hier_l4,
                            bfs.strategy_customer_hierachy_name
                        ORDER BY bfs.bu_version,
                            bfs.forecast_on_year,
                            bfs.forecast_on_month,
                            bfs.forecast_for_year,
                            bfs.forecast_for_mnth,
                            bfs.sls_ofc,
                            bfs.sls_ofc_desc,
                            bfs.sls_grp,
                            bfs.channel,
                            bfs.strategy_customer_hierachy_name,
                            prod_hier.prod_hier_l8,
                            prod_hier.prod_hier_l4
                    ) derivedtab2
                    LEFT JOIN 
                    (
                        SELECT 
                            'TWD'::character varying AS crncy_cd,
                            bfph.bu_version,
                            (bfph.forecast_on_year)::integer AS forecast_on_year,
                            (bfph.forecast_on_month)::integer AS forecast_on_month,
                            (bfph.forecast_for_year)::integer AS forecast_for_year,
                            CASE
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jan'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jan' IS NULL)
                                    )
                                ) THEN 1
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Feb'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Feb' IS NULL)
                                    )
                                ) THEN 2
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Mar'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Mar' IS NULL)
                                    )
                                ) THEN 3
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Apr'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Apr' IS NULL)
                                    )
                                ) THEN 4
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('May'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('May' IS NULL)
                                    )
                                ) THEN 5
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jun'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jun' IS NULL)
                                    )
                                ) THEN 6
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jul'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jul' IS NULL)
                                    )
                                ) THEN 7
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Aug'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Aug' IS NULL)
                                    )
                                ) THEN 8
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Sep'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Sep' IS NULL)
                                    )
                                ) THEN 9
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Oct'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Oct' IS NULL)
                                    )
                                ) THEN 10
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Nov'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Nov' IS NULL)
                                    )
                                ) THEN 11
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Dec'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Dec' IS NULL)
                                    )
                                ) THEN 12
                                ELSE NULL::integer
                            END AS forecast_for_mnth,
                            bfph.sls_grp,
                            bfph.channel,
                            bfph.sls_ofc,
                            bfph.sls_ofc_desc,
                            bfph.strategy_customer_hierachy_name,
                            bfph.lph_level_6,
                            sum(bfph.price_off) AS price_off,
                            sum(bfph.display) AS display,
                            sum(bfph.dm) AS dm,
                            sum(bfph.other_support) AS other_support,
                            sum(bfph.sr) AS sr,
                            sum(bfph.pre_sales_before_returns) AS pre_sales_before_returns,
                            sum(bfph.pre_sales) AS pre_sales
                        FROM 
                            (
                                SELECT a.bu_version,
                                    a.forecast_on_year,
                                    a.forecast_on_month,
                                    a.forecast_for_year,
                                    a.forecast_for_mnth,
                                    a.sls_grp,
                                    a.channel,
                                    a.sls_ofc,
                                    a.sls_ofc_desc,
                                    a.strategy_customer_hierachy_name,
                                    a.lph_level_6,
                                    a.price_off,
                                    a.display,
                                    a.dm,
                                    a.other_support,
                                    a.sr,
                                    a.pre_sales_before_returns,
                                    a.pre_sales,
                                    a.load_date
                                FROM 
                                    (
                                        (
                                            SELECT edw_tw_bu_forecast_prod_hier.bu_version,
                                                edw_tw_bu_forecast_prod_hier.forecast_on_year,
                                                edw_tw_bu_forecast_prod_hier.forecast_on_month,
                                                edw_tw_bu_forecast_prod_hier.forecast_for_year,
                                                edw_tw_bu_forecast_prod_hier.forecast_for_mnth,
                                                edw_tw_bu_forecast_prod_hier.sls_grp,
                                                edw_tw_bu_forecast_prod_hier.channel,
                                                edw_tw_bu_forecast_prod_hier.sls_ofc,
                                                edw_tw_bu_forecast_prod_hier.sls_ofc_desc,
                                                edw_tw_bu_forecast_prod_hier.strategy_customer_hierachy_name,
                                                edw_tw_bu_forecast_prod_hier.lph_level_6,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.price_off,
                                                    (0)::double precision
                                                ) AS price_off,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.display,
                                                    (0)::double precision
                                                ) AS display,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.dm,
                                                    (0)::double precision
                                                ) AS dm,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.other_support,
                                                    (0)::double precision
                                                ) AS other_support,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.sr,
                                                    (0)::double precision
                                                ) AS sr,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.pre_sales_before_returns,
                                                    (0)::double precision
                                                ) AS pre_sales_before_returns,
                                                COALESCE(
                                                    edw_tw_bu_forecast_prod_hier.pre_sales,
                                                    (0)::double precision
                                                ) AS pre_sales,
                                                edw_tw_bu_forecast_prod_hier.load_date
                                            FROM edw_tw_bu_forecast_prod_hier
                                        ) a
                                        JOIN 
                                        (
                                            SELECT edw_tw_bu_forecast_prod_hier.forecast_on_year,
                                                "max"(
                                                    (
                                                        (edw_tw_bu_forecast_prod_hier.forecast_on_year)::text || (edw_tw_bu_forecast_prod_hier.forecast_on_month)::text
                                                    )
                                                ) AS forecast_on_period
                                            FROM edw_tw_bu_forecast_prod_hier
                                            GROUP BY edw_tw_bu_forecast_prod_hier.forecast_on_year
                                        ) b ON 
                                        (
                                            (
                                                (
                                                    (a.forecast_on_year)::text = (b.forecast_on_year)::text
                                                )
                                                AND (
                                                    (
                                                        (a.forecast_on_year)::text || (a.forecast_on_month)::text
                                                    ) = b.forecast_on_period
                                                )
                                            )
                                        )
                                    )
                            ) bfph
                        GROUP BY 
                            1,
                            bfph.bu_version,
                            bfph.forecast_on_year,
                            bfph.forecast_on_month,
                            (bfph.forecast_for_year)::integer,
                            CASE
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jan'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jan' IS NULL)
                                    )
                                ) THEN 1
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Feb'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Feb' IS NULL)
                                    )
                                ) THEN 2
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Mar'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Mar' IS NULL)
                                    )
                                ) THEN 3
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Apr'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Apr' IS NULL)
                                    )
                                ) THEN 4
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('May'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('May' IS NULL)
                                    )
                                ) THEN 5
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jun'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jun' IS NULL)
                                    )
                                ) THEN 6
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jul'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jul' IS NULL)
                                    )
                                ) THEN 7
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Aug'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Aug' IS NULL)
                                    )
                                ) THEN 8
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Sep'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Sep' IS NULL)
                                    )
                                ) THEN 9
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Oct'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Oct' IS NULL)
                                    )
                                ) THEN 10
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Nov'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Nov' IS NULL)
                                    )
                                ) THEN 11
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Dec'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Dec' IS NULL)
                                    )
                                ) THEN 12
                                ELSE NULL::integer
                            END,
                            bfph.sls_grp,
                            bfph.channel,
                            bfph.sls_ofc,
                            bfph.sls_ofc_desc,
                            bfph.strategy_customer_hierachy_name,
                            bfph.lph_level_6
                        ORDER BY 
                            1,
                            bfph.bu_version,
                            bfph.forecast_on_year,
                            bfph.forecast_on_month,
                            (bfph.forecast_for_year)::integer,
                            CASE
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jan'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jan' IS NULL)
                                    )
                                ) THEN 1
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Feb'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Feb' IS NULL)
                                    )
                                ) THEN 2
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Mar'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Mar' IS NULL)
                                    )
                                ) THEN 3
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Apr'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Apr' IS NULL)
                                    )
                                ) THEN 4
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('May'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('May' IS NULL)
                                    )
                                ) THEN 5
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jun'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jun' IS NULL)
                                    )
                                ) THEN 6
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Jul'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Jul' IS NULL)
                                    )
                                ) THEN 7
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Aug'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Aug' IS NULL)
                                    )
                                ) THEN 8
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Sep'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Sep' IS NULL)
                                    )
                                ) THEN 9
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Oct'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Oct' IS NULL)
                                    )
                                ) THEN 10
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Nov'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Nov' IS NULL)
                                    )
                                ) THEN 11
                                WHEN (
                                    (
                                        (bfph.forecast_for_mnth)::text = ('Dec'::character varying)::text
                                    )
                                    OR (
                                        (bfph.forecast_for_mnth IS NULL)
                                        AND ('Dec' IS NULL)
                                    )
                                ) THEN 12
                                ELSE NULL::integer
                            END,
                            bfph.sls_grp,
                            bfph.channel,
                            bfph.sls_ofc,
                            bfph.sls_ofc_desc,
                            bfph.strategy_customer_hierachy_name,
                            bfph.lph_level_6
                    ) bfph ON 
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    (
                                                        (
                                                            (bfph.bu_version)::text = (derivedtab2.bu_version)::text
                                                        )
                                                        AND (
                                                            ((bfph.forecast_on_year)::character varying)::text = (derivedtab2.forecast_on_year)::text
                                                        )
                                                    )
                                                    AND (
                                                        ltrim(
                                                            ((bfph.forecast_on_month)::character varying)::text,
                                                            ('0'::character varying)::text
                                                        ) = ltrim(
                                                            (derivedtab2.forecast_on_month)::text,
                                                            ('0'::character varying)::text
                                                        )
                                                    )
                                                )
                                                AND (
                                                    ((bfph.forecast_for_year)::character varying)::text = (
                                                        (derivedtab2.forecast_for_year)::character varying
                                                    )::text
                                                )
                                            )
                                            AND (
                                                ((bfph.forecast_for_mnth)::character varying)::text = (
                                                    (derivedtab2.forecast_for_mnth)::character varying
                                                )::text
                                            )
                                        )
                                        AND (
                                            (
                                                COALESCE(bfph.lph_level_6, '#'::character varying)
                                            )::text = (
                                                COALESCE(
                                                    (derivedtab2.prod_hier_l8)::character varying,
                                                    '#'::character varying
                                                )
                                            )::text
                                        )
                                    )
                                    AND (
                                        (bfph.channel)::text = (derivedtab2.channel)::text
                                    )
                                )
                                AND (
                                    (bfph.sls_grp)::text = (derivedtab2.sls_grp)::text
                                )
                            )
                            AND (
                                (bfph.strategy_customer_hierachy_name)::text = (derivedtab2.strategy_customer_hierachy_name)::text
                            )
                        )
                    )
                )
                LEFT JOIN v_intrm_crncy_exch crncy ON 
                (
                    (
                        (derivedtab2.crncy_cd)::text = (crncy.from_crncy)::text
                    )
                )
            )
            LEFT JOIN edw_calendar_dim calendar ON 
            (
                (
                    calendar.cal_day = to_date(current_timestamp)
                )
            )
        )
    GROUP BY derivedtab2.crncy_cd,
        derivedtab2.bu_version,
        (derivedtab2.forecast_on_year)::integer,
        (derivedtab2.forecast_on_month)::integer,
        derivedtab2.forecast_for_year,
        derivedtab2.forecast_for_mnth,
        (to_char(current_date,('YYYYMMDD'::character varying)::text))::character varying,
        (
            (
                ((calendar.fisc_yr)::character varying)::text || "substring"(
                    ((calendar.fisc_per)::character varying)::text,
                    6
                )
            )
        )::character varying,
        derivedtab2.sls_grp,
        derivedtab2.channel,
        derivedtab2.sls_ofc,
        derivedtab2.sls_ofc_desc,
        derivedtab2.strategy_customer_hierachy_name,
        derivedtab2.prod_hier_l8,
        derivedtab2.prod_hier_l4,
        crncy.to_crncy,
        crncy.ex_rt
),
sapbw_actual as 
(
    SELECT 
        'SAPBW_ACTUAL' AS subsource_type,
        'TW' AS cntry_cd,
        derivedtab1.crncy_cd,
        '1' AS bu_version,
        null as forecast_on_year,
        null as forecast_on_month,
        derivedtab1.forecast_for_year,
        derivedtab1.forecast_for_month AS forecast_for_mnth,
        derivedtab1.caln_day,
        (to_char(current_date,('YYYYMMDD'::character varying)::text))::character varying AS latest_date,
        (
            (
                ((calendar.fisc_yr)::character varying)::text || "substring"(
                    ((calendar.fisc_per)::character varying)::text,
                    6
                )
            )
        )::character varying AS latest_fisc_yrmnth,
        (derivedtab1.sls_grp)::character varying AS sls_grp,
        (derivedtab1.channel)::character varying AS channel,
        (derivedtab1.sls_ofc)::character varying AS sls_ofc,
        (derivedtab1.sls_ofc_desc)::character varying AS sls_ofc_desc,
        (derivedtab1.strategy_customer_hierachy_name)::character varying AS strategy_customer_hierachy_name,
        (derivedtab1.prod_hier_l8)::character varying AS lph_level_6,
        0.0 AS gts_bp_tgt,
        0.0 AS gts_bu_forecast,
        sum(derivedtab1.gts_act) AS gts,
        sum(derivedtab1.term_and_logistics_others_act) AS trading_term_act,
        sum(derivedtab1.hidden_discount_prft_mergin_act) AS hidden_discnt_act,
        sum(derivedtab1.rtn_act) AS rtn_act,
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            (
                                                (
                                                    COALESCE(sum(derivedtab1.gts_act), (0)::double precision) - COALESCE(sum(derivedtab1.rtn_act), (0)::double precision)
                                                ) - COALESCE(
                                                    sum(derivedtab1.hidden_discount_prft_mergin_act),
                                                    (0)::double precision
                                                )
                                            ) - COALESCE(
                                                sum(
                                                    derivedtab1.hidden_discount_markdown_allowance_act
                                                ),
                                                (0)::double precision
                                            )
                                        ) - COALESCE(
                                            sum(derivedtab1.hidden_discount_promotion_act),
                                            (0)::double precision
                                        )
                                    ) - COALESCE(
                                        sum(derivedtab1.term_and_logistics_others_act),
                                        (0)::double precision
                                    )
                                ) - COALESCE(
                                    sum(derivedtab1.cash_dsct_act),
                                    (0)::double precision
                                )
                            ) - COALESCE(
                                sum(derivedtab1.logistics_fees_act),
                                (0)::double precision
                            )
                        ) - COALESCE(
                            sum(derivedtab1.profit_margin_allowance_act),
                            (0)::double precision
                        )
                    ) - COALESCE(
                        sum(derivedtab1.pricing_space_others_act),
                        (0)::double precision
                    )
                ) - COALESCE(
                    sum(derivedtab1.sales_allowance_act),
                    (0)::double precision
                )
            ) - COALESCE(
                sum(derivedtab1.volume_growth_funds_act),
                (0)::double precision
            )
        ) AS gts_act,
        0.0 AS nts_bp_tgt,
        0.0 AS nts_bu_forecast,
        COALESCE(sum(derivedtab1.nts_act), (0)::double precision) AS nts_act,
        0.0 AS tp_bp_tgt,
        0.0 AS tp_bu_forecast,
        COALESCE(
            (
                sum(derivedtab1.tp_act) * (- (1)::double precision)
            ),
            (0)::double precision
        ) AS tp_act,
        0.0 AS price_off_bu_forecast,
        0.0 AS display_bu_forecast,
        0.0 AS dm_bu_forecast,
        0.0 AS other_support_bu_forecast,
        0.0 AS sr_bu_forecast,
        (derivedtab1.prod_hier_l4)::character varying AS brand,
        crncy.to_crncy,
        crncy.ex_rt
    FROM 
        (
            (
                (
                    SELECT 'TWD'::character varying AS crncy_cd,
                        (
                            "left"(((copa.fisc_yr_per)::character varying)::text, 4)
                        )::integer AS forecast_for_year,
                        (
                            "right"(((copa.fisc_yr_per)::character varying)::text, 2)
                        )::integer AS forecast_for_month,
                        copa.caln_day,
                        copa.matl_no,
                        copa.cust_no,
                        copa.acct_hier_shrt_desc,
                        copa.acct_num,
                        COALESCE(
                            cust_flat.sls_grp,
                            ('#N/A'::character varying)::text
                        ) AS sls_grp,
                        COALESCE(
                            cust_flat.channel,
                            ('#N/A'::character varying)::text
                        ) AS channel,
                        COALESCE(
                            cust_flat.sls_ofc,
                            ('#N/A'::character varying)::text
                        ) AS sls_ofc,
                        COALESCE(
                            cust_flat.sls_ofc_desc,
                            ('#N/A'::character varying)::text
                        ) AS sls_ofc_desc,
                        COALESCE(
                            cust_attr.strategy_customer_hierachy_name,
                            ('#N/A'::character varying)::text
                        ) AS strategy_customer_hierachy_name,
                        prod_hier.prod_hier_l8,
                        prod_hier.prod_hier_l4,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('NTS'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS nts_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('GTS'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS gts_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('RTN'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS rtn_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('HDPM'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS hidden_discount_prft_mergin_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('HDMD'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS hidden_discount_markdown_allowance_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('HDPR'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS hidden_discount_promotion_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('TLO'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS term_and_logistics_others_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('CD'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS cash_dsct_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('LF'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS logistics_fees_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('PMA'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS profit_margin_allowance_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('PSO'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS pricing_space_others_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('SA'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS sales_allowance_act,
                        sum(
                            CASE
                                WHEN (
                                    (copa.acct_hier_shrt_desc)::text = ('VGF'::character varying)::text
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS volume_growth_funds_act,
                        sum(
                            CASE
                                WHEN (
                                    (
                                        (copa.acct_hier_shrt_desc)::text = ('NTS'::character varying)::text
                                    )
                                    AND (
                                        copa.acct_num IN (
                                            SELECT itg_query_parameters.parameter_value
                                            FROM itg_query_parameters
                                            WHERE (
                                                    (
                                                        (
                                                            (itg_query_parameters.country_code)::text = ('TW'::character varying)::text
                                                        )
                                                        AND (
                                                            (itg_query_parameters.parameter_name)::text = ('TP'::character varying)::text
                                                        )
                                                    )
                                                    AND (
                                                        (itg_query_parameters.parameter_type)::text = ('GL_Account'::character varying)::text
                                                    )
                                                )
                                        )
                                    )
                                ) THEN COALESCE(copa.amt_obj_crncy, (0)::double precision)
                                ELSE ((NULL::numeric)::numeric(18, 0))::double precision
                            END
                        ) AS tp_act
                    FROM 
                        (
                            (
                                (
                                    (
                                        SELECT edw_copa_trans_fact.caln_day,
                                            edw_copa_trans_fact.caln_yr_mo,
                                            edw_copa_trans_fact.fisc_yr_per,
                                            ltrim(
                                                (edw_copa_trans_fact.matl_num)::text,
                                                ('0'::character varying)::text
                                            ) AS matl_no,
                                            ltrim(
                                                (edw_copa_trans_fact.cust_num)::text,
                                                ('0'::character varying)::text
                                            ) AS cust_no,
                                            edw_copa_trans_fact.acct_hier_shrt_desc,
                                            edw_copa_trans_fact.acct_num,
                                            edw_copa_trans_fact.sls_vol,
                                            COALESCE(
                                                (edw_copa_trans_fact.amt_obj_crncy)::double precision,
                                                (0)::double precision
                                            ) AS amt_obj_crncy,
                                            edw_copa_trans_fact.fisc_yr
                                        FROM edw_copa_trans_fact
                                        WHERE (
                                                (
                                                    (edw_copa_trans_fact.sls_org)::text = ('1200'::character varying)::text
                                                )
                                                OR (
                                                    (edw_copa_trans_fact.sls_org)::text = ('120S'::character varying)::text
                                                )
                                            )
                                    ) copa
                                    LEFT JOIN (
                                        SELECT edw_customer_attr_hier_dim.sold_to_party,
                                            "max"(
                                                (
                                                    edw_customer_attr_hier_dim.strategy_customer_hierachy_name
                                                )::text
                                            ) AS strategy_customer_hierachy_name
                                        FROM edw_customer_attr_hier_dim
                                        WHERE (
                                                (edw_customer_attr_hier_dim.cntry)::text = ('Taiwan'::character varying)::text
                                            )
                                        GROUP BY edw_customer_attr_hier_dim.sold_to_party
                                        ORDER BY edw_customer_attr_hier_dim.sold_to_party
                                    ) cust_attr ON ((copa.cust_no = (cust_attr.sold_to_party)::text))
                                )
                                LEFT JOIN 
                                (
                                    SELECT edw_customer_attr_flat_dim.sold_to_party,
                                        "max"((edw_customer_attr_flat_dim.sls_grp)::text) AS sls_grp,
                                        "max"((edw_customer_attr_flat_dim.channel)::text) AS channel,
                                        "max"((edw_customer_attr_flat_dim.sls_ofc)::text) AS sls_ofc,
                                        "max"((edw_customer_attr_flat_dim.sls_ofc_desc)::text) AS sls_ofc_desc
                                    FROM edw_customer_attr_flat_dim
                                    WHERE (
                                            (edw_customer_attr_flat_dim.cntry)::text = ('Taiwan'::character varying)::text
                                        )
                                    GROUP BY edw_customer_attr_flat_dim.sold_to_party
                                ) cust_flat ON ((copa.cust_no = (cust_flat.sold_to_party)::text))
                            )
                            LEFT JOIN 
                            (
                                SELECT mat.matl_num,
                                    mat.base_prod_desc,
                                    "max"((prod_hier.prod_hier_l8)::text) AS prod_hier_l8,
                                    "max"((prod_hier.prod_hier_l4)::text) AS prod_hier_l4
                                FROM 
                                    (
                                        (
                                            SELECT copa.matl_num,
                                                mat_dim.ean_num AS ean,
                                                mat_dim.base_prod_desc
                                            FROM 
                                                (
                                                    (
                                                        SELECT DISTINCT ltrim(
                                                                (edw_copa_trans_fact.matl_num)::text,
                                                                ('0'::character varying)::text
                                                            ) AS matl_num
                                                        FROM edw_copa_trans_fact
                                                        WHERE (
                                                                (
                                                                    (edw_copa_trans_fact.sls_org)::text = ('1200'::character varying)::text
                                                                )
                                                                OR (
                                                                    (edw_copa_trans_fact.sls_org)::text = ('120S'::character varying)::text
                                                                )
                                                            )
                                                    ) copa
                                                    LEFT JOIN 
                                                    (
                                                        SELECT edw_material_sales_dim.sls_org,
                                                            edw_material_sales_dim.dstr_chnl,
                                                            edw_material_sales_dim.matl_num,
                                                            edw_material_sales_dim.matl_stats_grp,
                                                            edw_material_sales_dim.asrtmnt_grade,
                                                            edw_material_sales_dim.afs_vas_matl_grp,
                                                            edw_material_sales_dim.afs_prc_in,
                                                            edw_material_sales_dim.predecessor,
                                                            edw_material_sales_dim.sku_id,
                                                            edw_material_sales_dim.prodt_alloc_det_proc,
                                                            edw_material_sales_dim.num_pcs_in,
                                                            edw_material_sales_dim.ean_num,
                                                            edw_material_sales_dim.lcl_matl_grp_1,
                                                            edw_material_sales_dim.lcl_matl_grp_2,
                                                            edw_material_sales_dim.lcl_matl_grp_3,
                                                            edw_material_sales_dim.lcl_matl_grp_4,
                                                            edw_material_sales_dim.lcl_matl_grp_5,
                                                            edw_material_sales_dim.lcl_matl_grp_6,
                                                            edw_material_sales_dim.npi_in_apo,
                                                            edw_material_sales_dim.copy_hist,
                                                            edw_material_sales_dim.prod_classftn,
                                                            edw_material_sales_dim.fcst_indc_apo,
                                                            edw_material_sales_dim.prod_type_apo,
                                                            edw_material_sales_dim.mstr_cd,
                                                            edw_material_sales_dim.med_desc,
                                                            edw_material_sales_dim.crt_dttm,
                                                            edw_material_sales_dim.updt_dttm,
                                                            emd.base_prod_desc
                                                        FROM (
                                                                edw_material_sales_dim
                                                                LEFT JOIN edw_material_dim emd ON (
                                                                    (
                                                                        (edw_material_sales_dim.matl_num)::text = (emd.matl_num)::text
                                                                    )
                                                                )
                                                            )
                                                        WHERE (
                                                                (
                                                                    (edw_material_sales_dim.sls_org)::text = ('1200'::character varying)::text
                                                                )
                                                                OR (
                                                                    (edw_material_sales_dim.sls_org)::text = ('120S'::character varying)::text
                                                                )
                                                            )
                                                    ) mat_dim ON (
                                                        (
                                                            copa.matl_num = ltrim(
                                                                (mat_dim.matl_num)::text,
                                                                ('0'::character varying)::text
                                                            )
                                                        )
                                                    )
                                                )
                                        ) mat
                                        LEFT JOIN (
                                            SELECT DISTINCT edw_product_attr_dim.ean,
                                                edw_product_attr_dim.prod_hier_l4,
                                                edw_product_attr_dim.prod_hier_l8
                                            FROM edw_product_attr_dim
                                            WHERE (
                                                    (edw_product_attr_dim.cntry)::text = ('TW'::character varying)::text
                                                )
                                        ) prod_hier ON (((mat.ean)::text = (prod_hier.ean)::text))
                                    )
                                GROUP BY mat.matl_num,
                                    mat.base_prod_desc
                                ORDER BY mat.matl_num
                            ) prod_hier ON (
                                (
                                    copa.matl_no = ((prod_hier.matl_num)::character varying)::text
                                )
                            )
                        )
                    WHERE (
                            (
                                (prod_hier.base_prod_desc)::text <> ('DR CI LABO Series'::character varying)::text
                            )
                            OR (prod_hier.base_prod_desc IS NULL)
                        )
                    GROUP BY copa.caln_day,
                        copa.caln_yr_mo,
                        copa.fisc_yr_per,
                        copa.matl_no,
                        copa.cust_no,
                        copa.acct_hier_shrt_desc,
                        copa.acct_num,
                        cust_flat.sls_grp,
                        cust_flat.channel,
                        cust_flat.sls_ofc,
                        cust_flat.sls_ofc_desc,
                        cust_attr.strategy_customer_hierachy_name,
                        prod_hier.prod_hier_l8,
                        prod_hier.prod_hier_l4
                    ORDER BY copa.caln_day,
                        copa.caln_yr_mo,
                        copa.fisc_yr_per,
                        copa.matl_no,
                        copa.cust_no,
                        copa.acct_hier_shrt_desc,
                        copa.acct_num,
                        cust_flat.sls_grp,
                        cust_flat.channel,
                        cust_flat.sls_ofc,
                        cust_flat.sls_ofc_desc,
                        cust_attr.strategy_customer_hierachy_name,
                        prod_hier.prod_hier_l8,
                        prod_hier.prod_hier_l4
                ) derivedtab1
                LEFT JOIN v_intrm_crncy_exch crncy ON
                (
                    (
                        (derivedtab1.crncy_cd)::text = (crncy.from_crncy)::text
                    )
                )
            )
            LEFT JOIN edw_calendar_dim calendar ON (
                (
                    calendar.cal_day = to_date(current_timestamp)
                )
            )
        )
    GROUP BY 
        derivedtab1.crncy_cd,
        derivedtab1.forecast_for_year,
        derivedtab1.forecast_for_month,
        derivedtab1.caln_day,
        derivedtab1.prod_hier_l8,
        derivedtab1.prod_hier_l4,
        (to_char(current_date,('YYYYMMDD'::character varying)::text))::character varying,
        (
            (
                ((calendar.fisc_yr)::character varying)::text || "substring"(
                    ((calendar.fisc_per)::character varying)::text,
                    6
                )
            )
        )::character varying,
        derivedtab1.sls_grp,
        derivedtab1.channel,
        derivedtab1.sls_ofc,
        derivedtab1.strategy_customer_hierachy_name,
        derivedtab1.sls_ofc_desc,
        crncy.to_crncy,
        crncy.ex_rt
    ORDER BY 
        derivedtab1.crncy_cd,
        derivedtab1.forecast_for_year,
        derivedtab1.forecast_for_month,
        derivedtab1.caln_day,
        derivedtab1.prod_hier_l8,
        derivedtab1.prod_hier_l4,
        derivedtab1.sls_grp,
        derivedtab1.channel,
        derivedtab1.sls_ofc,
        derivedtab1.strategy_customer_hierachy_name,
        derivedtab1.sls_ofc_desc,
        crncy.to_crncy,
        crncy.ex_rt
),
bp_target as 
(
    SELECT 
        'BP_TARGET'::character varying AS subsource_type,
        'TW'::character varying AS cntry_cd,
        derivedtable1.crncy_cd,
        derivedtable1.bp_version AS bu_version,
        (derivedtable1.forecast_on_year)::integer AS forecast_on_year,
        (derivedtable1.forecast_on_month)::integer AS forecast_on_month,
        derivedtable1.forecast_for_year,
        derivedtable1.forecast_for_mnth,
        null as caln_day,
        (to_char(current_date,('YYYYMMDD'::character varying)::text))::character varying AS latest_date,
        (
            (
                ((calendar.fisc_yr)::character varying)::text || "substring"(
                    ((calendar.fisc_per)::character varying)::text,
                    6
                )
            )
        )::character varying AS latest_fisc_yrmnth,
        derivedtable1.sls_grp,
        derivedtable1.channel,
        derivedtable1.sls_ofc,
        derivedtable1.sls_ofc_desc,
        derivedtable1.strategy_customer_hierachy_name,
        derivedtable1.lph_level_6,
        sum(derivedtable1.gts_bp_tgt) AS gts_bp_tgt,
        null as gts_bu_forecast,
        null as gts,
        null as trading_term_act,
        null as hidden_discnt_act,
        null as rtn_act,
        null as gts_act,
        sum(derivedtable1.nts_bp_tgt) AS nts_bp_tgt,
        null as nts_bu_forecast,
        null as nts_act,
        sum(derivedtable1.tp_bp_tgt) AS tp_bp_tgt,
        null as tp_bu_forecast,
        null as tp_act,
        null as price_off_bu_forecast,
        null as display_bu_forecast,
        null as dm_bu_forecast,
        null as other_support_bu_forecast,
        null as sr_bu_forecast,
        prod_hier.prod_hier_l4 AS brand,
        crncy.to_crncy,
        crncy.ex_rt
    FROM 
        (
            (
                (
                    (
                        SELECT 'TWD'::character varying AS crncy_cd,
                            bptgt.bp_version,
                            bptgt.forecast_on_year,
                            bptgt.forecast_on_month,
                            (bptgt.forecast_for_year)::integer AS forecast_for_year,
                            CASE
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Jan'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Jan' IS NULL)
                                    )
                                ) THEN 1
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Feb'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Feb' IS NULL)
                                    )
                                ) THEN 2
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Mar'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Mar' IS NULL)
                                    )
                                ) THEN 3
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Apr'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Apr' IS NULL)
                                    )
                                ) THEN 4
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('May'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('May' IS NULL)
                                    )
                                ) THEN 5
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Jun'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Jun' IS NULL)
                                    )
                                ) THEN 6
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Jul'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Jul' IS NULL)
                                    )
                                ) THEN 7
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Aug'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Aug' IS NULL)
                                    )
                                ) THEN 8
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Sep'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Sep' IS NULL)
                                    )
                                ) THEN 9
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Oct'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Oct' IS NULL)
                                    )
                                ) THEN 10
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Nov'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Nov' IS NULL)
                                    )
                                ) THEN 11
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Dec'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Dec' IS NULL)
                                    )
                                ) THEN 12
                                ELSE NULL::integer
                            END AS forecast_for_mnth,
                            bptgt.sls_grp,
                            bptgt.channel,
                            bptgt.sls_ofc,
                            bptgt.sls_ofc_desc,
                            bptgt.strategy_customer_hierachy_name,
                            bptgt.lph_level_6,
                            sum(bptgt.pre_sales) AS gts_bp_tgt,
                            sum(bptgt.nts) AS nts_bp_tgt,
                            sum(bptgt.tp) AS tp_bp_tgt
                        FROM edw_tw_bp_target bptgt
                        GROUP BY 1,
                            bptgt.bp_version,
                            bptgt.forecast_on_year,
                            bptgt.forecast_on_month,
                            bptgt.forecast_for_year,
                            bptgt.forecast_for_mnth,
                            bptgt.sls_grp,
                            bptgt.channel,
                            bptgt.sls_ofc,
                            bptgt.sls_ofc_desc,
                            bptgt.strategy_customer_hierachy_name,
                            bptgt.lph_level_6
                        ORDER BY 1,
                            bptgt.bp_version,
                            bptgt.forecast_on_year,
                            bptgt.forecast_on_month,
                            (bptgt.forecast_for_year)::integer,
                            CASE
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Jan'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Jan' IS NULL)
                                    )
                                ) THEN 1
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Feb'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Feb' IS NULL)
                                    )
                                ) THEN 2
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Mar'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Mar' IS NULL)
                                    )
                                ) THEN 3
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Apr'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Apr' IS NULL)
                                    )
                                ) THEN 4
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('May'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('May' IS NULL)
                                    )
                                ) THEN 5
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Jun'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Jun' IS NULL)
                                    )
                                ) THEN 6
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Jul'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Jul' IS NULL)
                                    )
                                ) THEN 7
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Aug'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Aug' IS NULL)
                                    )
                                ) THEN 8
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Sep'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Sep' IS NULL)
                                    )
                                ) THEN 9
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Oct'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Oct' IS NULL)
                                    )
                                ) THEN 10
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Nov'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Nov' IS NULL)
                                    )
                                ) THEN 11
                                WHEN (
                                    (
                                        (bptgt.forecast_for_mnth)::text = ('Dec'::character varying)::text
                                    )
                                    OR (
                                        (bptgt.forecast_for_mnth IS NULL)
                                        AND ('Dec' IS NULL)
                                    )
                                ) THEN 12
                                ELSE NULL::integer
                            END,
                            bptgt.sls_grp,
                            bptgt.channel,
                            bptgt.sls_ofc,
                            bptgt.sls_ofc_desc,
                            bptgt.strategy_customer_hierachy_name,
                            bptgt.lph_level_6
                    ) derivedtable1
                    LEFT JOIN (
                        SELECT DISTINCT edw_product_attr_dim.prod_hier_l4,
                            edw_product_attr_dim.prod_hier_l8
                        FROM edw_product_attr_dim
                        WHERE (
                                (edw_product_attr_dim.cntry)::text = ('TW'::character varying)::text
                            )
                    ) prod_hier ON (
                        (
                            (derivedtable1.lph_level_6)::text = (prod_hier.prod_hier_l8)::text
                        )
                    )
                )
                LEFT JOIN v_intrm_crncy_exch crncy ON (
                    (
                        (derivedtable1.crncy_cd)::text = (crncy.from_crncy)::text
                    )
                )
            )
            LEFT JOIN edw_calendar_dim calendar ON (
                (
                    calendar.cal_day = to_date(current_timestamp)
                )
            )
        )
    GROUP BY 1,
        2,
        derivedtable1.crncy_cd,
        derivedtable1.bp_version,
        derivedtable1.forecast_on_year,
        derivedtable1.forecast_on_month,
        derivedtable1.forecast_for_year,
        derivedtable1.forecast_for_mnth,
        (to_char(current_date,('YYYYMMDD'::character varying)::text))::character varying,
        (
            (
                ((calendar.fisc_yr)::character varying)::text || "substring"(
                    ((calendar.fisc_per)::character varying)::text,
                    6
                )
            )
        )::character varying,
        derivedtable1.sls_grp,
        derivedtable1.channel,
        derivedtable1.sls_ofc,
        derivedtable1.strategy_customer_hierachy_name,
        derivedtable1.sls_ofc_desc,
        derivedtable1.lph_level_6,
        prod_hier.prod_hier_l4,
        crncy.to_crncy,
        crncy.ex_rt
    ORDER BY 1,
        2,
        derivedtable1.crncy_cd,
        derivedtable1.bp_version,
        (derivedtable1.forecast_on_year)::integer,
        (derivedtable1.forecast_on_month)::integer,
        derivedtable1.sls_grp,
        derivedtable1.channel,
        derivedtable1.sls_ofc,
        derivedtable1.strategy_customer_hierachy_name,
        derivedtable1.sls_ofc_desc,
        derivedtable1.lph_level_6,
        prod_hier.prod_hier_l4,
        crncy.to_crncy,
        crncy.ex_rt
),
final as 
(
    select * from bu_forecast
    union all
    select * from sapbw_actual
    union all
    select * from bp_target
)
select * from final