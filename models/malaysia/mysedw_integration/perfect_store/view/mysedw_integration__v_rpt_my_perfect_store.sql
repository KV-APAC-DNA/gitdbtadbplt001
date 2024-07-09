with edw_vw_ps_targets as
(
    select * from {{ ref('aspedw_integration__edw_vw_ps_targets') }}
),
edw_vw_ps_weights as
(
    select * from {{ ref('aspedw_integration__edw_vw_ps_weights') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_my_sellout_analysis as
(
    select * from {{ ref('mysedw_integration__edw_my_sellout_analysis') }}
),
edw_vw_my_ps_osa as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_ps_osa') }}
),
edw_vw_my_ps_outlet_mst as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_ps_outlet_mst') }}
),
edw_vw_my_ps_promocomp as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_ps_promocomp') }}
),
edw_vw_my_ps_sku_mst as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_ps_sku_mst') }}
),
edw_vw_my_ps_sos as
(
    select * from {{ ref('mysedw_integration__edw_vw_my_ps_sos') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_my_ps_msl as
(
    select * from {{ ref('mysitg_integration__itg_mds_my_ps_msl') }}
),
itg_my_dstrbtr_cust_dim as
(
    select * from {{ ref('mysitg_integration__itg_my_dstrbtr_cust_dim') }}
),
itg_my_material_dim as
(
    select * from {{ ref('mysitg_integration__itg_my_material_dim') }}
),
itg_my_dstrbtrr_dim as 
(
    select * from {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }}
),
merchandising_response_gt_msl as
(
    SELECT 
        'Merchandising_Response' AS dataset,
        (
            (
                (
                    (emsa.store_code)::text || ('-'::character varying)::text
                ) || (emsa.distributor_code)::text
            )
        )::character varying AS customerid,
        CASE
            WHEN (
                (emsa.slsmn_cd IS NULL)
                OR (
                    (emsa.slsmn_cd)::text = (''::character varying)::text
                )
            ) THEN 'Not Assigned'::character varying
            ELSE emsa.slsmn_cd
        END AS salespersonid,
        'true' AS mustcarryitem,
        null AS answerscore,
        CASE
            WHEN (sum(emsa.msl_hit) > 0) THEN 'true'::character varying
            ELSE 'false'::character varying
        END AS presence,
        null AS outofstock,
        'MSL Compliance' AS kpi,
        to_date(
            (
                (
                    (
                        "substring"(((emsa.l1_month)::character varying)::text, 0, 4) || ('-'::character varying)::text
                    ) || "substring"(((emsa.l1_month)::character varying)::text, 5, 7)
                ) || ('-15'::character varying)::text
            ),
            ('YYYY-MM-DD'::character varying)::text
        ) AS scheduleddate,
        'completed' AS vst_status,
        (
            "left"(((emsa.l1_month)::character varying)::text, 4)
        )::character varying AS fisc_yr,
        emsa.l1_month AS fisc_per,
        COALESCE(emsa.slsmn_nm, ''::character varying) AS firstname,
        '' AS lastname,
        (
            "max"(
                (
                    (
                        (
                            (
                                (COALESCE(emsa.store_name, ''::character varying))::text || ('-'::character varying)::text
                            ) || (COALESCE(emsa.store_code, ''::character varying))::text
                        ) || ('-'::character varying)::text
                    ) || (
                        COALESCE(emsa.distributor_code, ''::character varying)
                    )::text
                )
            )
        )::character varying AS customername,
        'Malaysia' AS country,
        (upper(emsa.region_nm))::character varying AS state,
        emsa.distributor_name AS storereference,
        (upper((emsa.retail_environment)::text))::character varying AS storetype,
        'GT' AS channel,
        emsa.retailer_name AS salesgroup,
        'Malaysia' AS prod_hier_l1,
        null AS prod_hier_l2,
        (emsa.frnchse_desc)::character varying AS prod_hier_l3,
        (emsa.brnd_desc)::character varying AS prod_hier_l4,
        (emsa.vrnt_desc)::character varying AS prod_hier_l5,
        (emsa.putup_desc)::character varying AS prod_hier_l6,
        null AS prod_hier_l7,
        null AS prod_hier_l8,
        emsa.product_name AS prod_hier_l9,
        (wt.weight)::double precision AS kpi_chnl_wt,
        null AS "y/n_flag",
        null AS posm_execution_flag,
        'Y' AS priority_store_flag,
        null AS questiontext,
        null AS ques_desc,
        null AS value,
        null AS mkt_share,
        null AS rej_reason
    FROM 
        (
            SELECT 
                derived_table2.l1_month,
                derived_table2.retail_environment,
                derived_table2.distributor_code,
                derived_table2.distributor_name,
                derived_table2.retailer_name,
                derived_table2.store_code,
                derived_table2.store_name,
                derived_table2.ean,
                derived_table2.product_name,
                derived_table2.slsmn_cd,
                derived_table2.slsmn_nm,
                derived_table2.region_nm,
                derived_table2.frnchse_desc,
                derived_table2.brnd_desc,
                derived_table2.vrnt_desc,
                derived_table2.putup_desc,
                "max"(derived_table2.msl_flag) AS msl_flag,
                "max"(derived_table2.msl_hit) AS msl_hit
            FROM 
                (
                    SELECT 
                        rtrim(month.l1_month) as l1_month,
                        a.retail_environment,
                        rtrim(a.distributor_code) as distributor_code,
                        rtrim(a.distributor_name) as distributor_name,
                        rtrim(a.retailer_name) as retailer_name,
                        rtrim(a.store_code) as store_code,
                        rtrim(a.store_name) as store_name,
                        rtrim(a.ean) as ean,
                        rtrim(a.product_name) as product_name,
                        rtrim(a.slsmn_cd) as slsmn_cd,
                        rtrim(a.slsmn_nm) as slsmn_nm,
                        rtrim(a.region_nm) as region_nm,
                        rtrim(a.frnchse_desc) as frnchse_desc,
                        rtrim(a.brnd_desc) as brnd_desc,
                        rtrim(a.vrnt_desc) as vrnt_desc,
                        rtrim(a.putup_desc) as putup_desc,
                        rtrim(a.msl_flag) as msl_flag,
                        CASE
                            WHEN (sum(a.msl_hit) > ((0)::numeric)::numeric(18, 0)) THEN 1
                            ELSE 0
                        END AS msl_hit
                    FROM 
                        (
                            SELECT 
                                msl.mnth_id,
                                msl.retail_environment,
                                msl.distributor_code,
                                msl.distributor_name,
                                msl.retailer_name,
                                msl.store_code,
                                msl.store_name,
                                msl.ean,
                                msl.product_name,
                                msl.slsmn_cd,
                                msl.slsmn_nm,
                                COALESCE(msl.state, (trans.region_nm)::text) AS region_nm,
                                COALESCE(msl.prod_hier_l3, (trans.frnchse_desc)::text) AS frnchse_desc,
                                COALESCE(msl.prod_hier_l4, (trans.brnd_desc)::text) AS brnd_desc,
                                COALESCE(msl.prod_hier_l5, (trans.vrnt_desc)::text) AS vrnt_desc,
                                COALESCE(msl.prod_hier_l6, (trans.putup_desc)::text) AS putup_desc,
                                msl.msl_flag,
                                CASE
                                    WHEN (trans.net_trd_sls IS NULL) THEN ((0)::numeric)::numeric(18, 0)
                                    ELSE trans.net_trd_sls
                                END AS msl_hit
                            FROM (
                                    (
                                        SELECT 
                                            derived_table1.cal_mnth_id AS mnth_id,
                                            "max"((a.region)::text) AS state,
                                            a.outlet_type1 AS retail_environment,
                                            a.cust_id AS distributor_code,
                                            a.cust_nm AS retailer_name,
                                            a.outlet_id AS store_code,
                                            a.outlet_desc AS store_name,
                                            a.ean,
                                            a.prod_nm AS product_name,
                                            1 AS msl_flag,
                                            a.slsmn_cd,
                                            "max"((d.frnchse_desc)::text) AS prod_hier_l3,
                                            "max"((d.brnd_desc)::text) AS prod_hier_l4,
                                            "max"((d.vrnt_desc)::text) AS prod_hier_l5,
                                            "max"((d.putup_desc)::text) AS prod_hier_l6,
                                            a.lvl2 AS distributor_name,
                                            a.cdm AS slsmn_nm
                                        FROM 
                                            (
                                                (
                                                    (
                                                        SELECT 
                                                            a.channel_nm,
                                                            a.ean,
                                                            a.prod_nm,
                                                            a.crtd_dttm,
                                                            a.valid_from,
                                                            a.valid_to,
                                                            c.outlet_key,
                                                            c.outlet_id,
                                                            c.outlet_desc,
                                                            c.outlet_type1,
                                                            c.outlet_type2,
                                                            c.outlet_type3,
                                                            c.outlet_type4,
                                                            c.town,
                                                            c.cust_year,
                                                            c.slsmn_cd,
                                                            dist.cust_id,
                                                            dist.cust_nm,
                                                            dist.lvl1,
                                                            dist.lvl2,
                                                            dist.lvl3,
                                                            dist.lvl4,
                                                            dist.lvl5,
                                                            dist.trdng_term_val,
                                                            dist.abbrevation,
                                                            dist.buyer_gln,
                                                            dist.location_gln,
                                                            dist.chnl_manager,
                                                            dist.cdm,
                                                            dist.region,
                                                            dist.cdl_dttm,
                                                            dist.crtd_dttm,
                                                            dist.updt_dttm
                                                        FROM 
                                                            (
                                                                (
                                                                    {{ ref('mysitg_integration__itg_mds_my_ps_msl') }} a
                                                                    JOIN {{ ref('mysitg_integration__itg_my_dstrbtr_cust_dim') }} c ON (
                                                                        (
                                                                            rtrim(upper((c.outlet_type1)::text)) = rtrim(upper((a.channel_nm)::text))
                                                                        )
                                                                    )
                                                                )
                                                                JOIN {{ ref('mysitg_integration__itg_my_dstrbtrr_dim') }} dist ON (((c.cust_id)::text = (dist.cust_id)::text))
                                                            )
                                                        WHERE 
                                                            (
                                                                (dist.lvl5)::text = ('Active'::character varying)::text
                                                            )
                                                    ) a
                                                    LEFT JOIN {{ ref('mysitg_integration__itg_my_material_dim') }} d ON (
                                                        (
                                                            ltrim((a.ean)::text, ('0'::character varying)::text) = ltrim(
                                                                (d.item_bar_cd)::text,
                                                                ('0'::character varying)::text
                                                            )
                                                        )
                                                    )
                                                )
                                                JOIN 
                                                (
                                                    SELECT DISTINCT edw_vw_os_time_dim.cal_mnth_id
                                                    FROM edw_vw_os_time_dim
                                                    WHERE (
                                                            (
                                                                "substring"(
                                                                    (
                                                                        (edw_vw_os_time_dim.cal_mnth_id)::character varying
                                                                    )::text,
                                                                    0,
                                                                    4
                                                                ) >= (
                                                                    (
                                                                        (
                                                                            "date_part"(
                                                                                year,
                                                                                convert_timezone('UTC', current_timestamp())::timestamp without time zone
                                                                            ) - 1
                                                                        )
                                                                    )::character varying
                                                                )::text
                                                            )
                                                            AND (
                                                                (
                                                                    (edw_vw_os_time_dim.cal_mnth_id)::character varying
                                                                )::text <= "substring"(
                                                                    "replace"(
                                                                        (
                                                                            (
                                                                                to_date(
                                                                                    convert_timezone('UTC', current_timestamp())::timestamp without time zone
                                                                                )
                                                                            )::character varying
                                                                        )::text,
                                                                        ('-'::character varying)::text,
                                                                        (''::character varying)::text
                                                                    ),
                                                                    0,
                                                                    6
                                                                )
                                                            )
                                                        )
                                                ) derived_table1 ON 
                                                (
                                                    (
                                                        (
                                                            ((derived_table1.cal_mnth_id)::character varying)::text >= "replace"(
                                                                "substring"(((a.valid_from)::character varying)::text, 0, 7),
                                                                ('-'::character varying)::text,
                                                                (''::character varying)::text
                                                            )
                                                        )
                                                        AND (
                                                            ((derived_table1.cal_mnth_id)::character varying)::text <= "replace"(
                                                                "substring"(((a.valid_to)::character varying)::text, 0, 7),
                                                                ('-'::character varying)::text,
                                                                (''::character varying)::text
                                                            )
                                                        )
                                                    )
                                                )
                                            )
                                        GROUP BY derived_table1.cal_mnth_id,
                                            a.outlet_type1,
                                            a.cust_id,
                                            a.cust_nm,
                                            a.outlet_id,
                                            a.outlet_desc,
                                            a.ean,
                                            a.prod_nm,
                                            a.slsmn_cd,
                                            a.lvl2,
                                            a.cdm
                                    ) msl
                                    LEFT JOIN 
                                    (
                                        SELECT 
                                            rtrim(edw_my_sellout_analysis.mnth_id) as mnth_id,
                                            rtrim(edw_my_sellout_analysis.dstrbtr_grp_cd) as dstrbtr_grp_cd,
                                            rtrim(edw_my_sellout_analysis.dstrbtr_lvl5) as dstrbtr_lvl5,
                                            rtrim(edw_my_sellout_analysis.dstrbtr_cust_cd) as dstrbtr_cust_cd,
                                            rtrim(edw_my_sellout_analysis.sap_soldto_code) as sap_soldto_code,
                                            rtrim(edw_my_sellout_analysis.region_nm) as region_nm,
                                            rtrim(edw_my_sellout_analysis.frnchse_desc) as frnchse_desc,
                                            rtrim(edw_my_sellout_analysis.brnd_desc) as brnd_desc,
                                            rtrim(edw_my_sellout_analysis.vrnt_desc) as vrnt_desc,
                                            rtrim(edw_my_sellout_analysis.putup_desc) as putup_desc,
                                            rtrim(edw_my_sellout_analysis.bar_cd) as bar_cd,
                                            rtrim(edw_my_sellout_analysis.chnl_desc) as chnl_desc,
                                            sum(edw_my_sellout_analysis.net_trd_sls) AS net_trd_sls
                                        FROM edw_my_sellout_analysis
                                        WHERE (
                                                (
                                                    (edw_my_sellout_analysis.from_ccy)::text = ('MYR'::character varying)::text
                                                )
                                                AND (
                                                    (edw_my_sellout_analysis.to_ccy)::text = ('MYR'::character varying)::text
                                                )
                                            )
                                        GROUP BY
                                            rtrim(edw_my_sellout_analysis.mnth_id),
                                            rtrim(edw_my_sellout_analysis.dstrbtr_grp_cd),
                                            rtrim(edw_my_sellout_analysis.dstrbtr_lvl5),
                                            rtrim(edw_my_sellout_analysis.dstrbtr_cust_cd),
                                            rtrim(edw_my_sellout_analysis.sap_soldto_code),
                                            rtrim(edw_my_sellout_analysis.region_nm),
                                            rtrim(edw_my_sellout_analysis.frnchse_desc),
                                            rtrim(edw_my_sellout_analysis.brnd_desc),
                                            rtrim(edw_my_sellout_analysis.vrnt_desc),
                                            rtrim(edw_my_sellout_analysis.putup_desc),
                                            rtrim(edw_my_sellout_analysis.bar_cd),
                                            rtrim(edw_my_sellout_analysis.chnl_desc)
                                    ) trans ON (
                                        (
                                            (
                                                (
                                                    (
                                                        (trans.sap_soldto_code)::text = (msl.distributor_code)::text
                                                    )
                                                    AND (
                                                        (trans.dstrbtr_cust_cd)::text = (msl.store_code)::text
                                                    )
                                                )
                                                AND ((trans.bar_cd)::text = (msl.ean)::text)
                                            )
                                            AND (trans.mnth_id = msl.mnth_id)
                                        )
                                    )
                                )
                        ) a,
                        (
                            SELECT DISTINCT edw_calendar_dim.cal_yr AS "year",
                                edw_calendar_dim.cal_mo_1 AS l1_month,
                                "left"(
                                    (
                                        (
                                            "replace"(
                                                (
                                                    (
                                                        to_date(
                                                            add_months(
                                                                (
                                                                    to_date(
                                                                        (
                                                                            (edw_calendar_dim.cal_mo_1)::character varying(10)
                                                                        )::text,
                                                                        ('YYYYMM'::character varying)::text
                                                                    )
                                                                )::timestamp without time zone,
                                                                (- (2)::bigint)
                                                            )
                                                        )
                                                    )::character varying
                                                )::text,
                                                ('-'::character varying)::text,
                                                (''::character varying)::text
                                            )
                                        )::character varying(10)
                                    )::text,
                                    6
                                ) AS l3_month
                            FROM edw_calendar_dim
                        ) month
                    WHERE (
                            (
                                ((a.mnth_id)::character varying)::text >= month.l3_month
                            )
                            AND (a.mnth_id <= month.l1_month)
                        )
                    GROUP BY 
                        rtrim(month.l1_month),
                        (a.retail_environment),
                        rtrim(a.distributor_code),
                        rtrim(a.distributor_name),
                        rtrim(a.retailer_name),
                        rtrim(a.store_code),
                        rtrim(a.store_name),
                        rtrim(a.ean),
                        rtrim(a.product_name),
                        rtrim(a.slsmn_cd),
                        rtrim(a.slsmn_nm),
                        rtrim(a.region_nm),
                        rtrim(a.frnchse_desc),
                        rtrim(a.brnd_desc),
                        rtrim(a.vrnt_desc),
                        rtrim(a.putup_desc),
                        rtrim(a.msl_flag)
                ) derived_table2
            GROUP BY derived_table2.l1_month,
                derived_table2.retail_environment,
                derived_table2.distributor_code,
                derived_table2.distributor_name,
                derived_table2.retailer_name,
                derived_table2.store_code,
                derived_table2.store_name,
                derived_table2.ean,
                derived_table2.product_name,
                derived_table2.slsmn_cd,
                derived_table2.slsmn_nm,
                derived_table2.region_nm,
                derived_table2.frnchse_desc,
                derived_table2.brnd_desc,
                derived_table2.vrnt_desc,
                derived_table2.putup_desc
        ) emsa,
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = ('MSL COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_weights.channel)::text) = ('GT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_weights.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) wt
    WHERE 
        (
            (
                (
                    rtrim(upper((wt.retail_environment)::text)) = COALESCE(
                        upper((rtrim(emsa.retail_environment))::text),
                        ('#'::character varying)::text
                    )
                )
                AND (
                    "substring"(((emsa.l1_month)::character varying)::text, 0, 4) >= (
                        (
                            (
                                "date_part"(
                                    year,
                                    convert_timezone('UTC', current_timestamp())::timestamp without time zone
                                ) - 1
                            )
                        )::character varying
                    )::text
                )
            )
            AND (
                "substring"(((emsa.l1_month)::character varying)::text, 0, 6) <= to_char(
                    convert_timezone('UTC', current_timestamp())::timestamp without time zone,
                    ('YYYYMM'::character varying)::text
                )
            )
        )
    GROUP BY 
        emsa.store_code,
        CASE
            WHEN (
                (emsa.slsmn_cd IS NULL)
                OR (
                    (emsa.slsmn_cd)::text = (''::character varying)::text
                )
            ) THEN 'Not Assigned'::character varying
            ELSE emsa.slsmn_cd
        END,
        "left"(((emsa.l1_month)::character varying)::text, 4),
        emsa.l1_month,
        emsa.slsmn_nm,
        emsa.store_name,
        emsa.region_nm,
        (upper(emsa.region_nm))::character varying,
        emsa.distributor_code,
        emsa.distributor_name,
        emsa.retailer_name,
        emsa.retail_environment,
        emsa.frnchse_desc,
        emsa.brnd_desc,
        emsa.vrnt_desc,
        emsa.putup_desc,
        emsa.product_name,
        (wt.weight)::double precision
),
survey_response_promo as
(
    SELECT
        'Survey_Response' AS dataset,
        prmc.outlet_no AS customerid,
        '' AS salespersonid,
        null as mustcarryitem,
        null as answerscore,
        null as presence,
        null as outofstock,
        'PROMO COMPLIANCE' AS kpi,
        to_date(
            (
                (
                    to_date(
                        (prm.visit_datetime)::timestamp without time zone
                    )
                )::character varying
            )::text,
            ('YYYY-MM-DD'::character varying)::text
        ) AS scheduleddate,
        'completed' AS vst_status,
        (
            "substring"(
                "replace"(
                    (
                        (
                            to_date(
                                (prm.visit_datetime)::timestamp without time zone
                            )
                        )::character varying
                    )::text,
                    ('-'::character varying)::text,
                    (''::character varying)::text
                ),
                0,
                4
            )
        )::character varying AS fisc_yr,
        (
            "substring"(
                "replace"(
                    (
                        (
                            to_date(
                                (prm.visit_datetime)::timestamp without time zone
                            )
                        )::character varying
                    )::text,
                    ('-'::character varying)::text,
                    (''::character varying)::text
                ),
                0,
                6
            )
        )::integer AS fisc_per,
        '' AS firstname,
        '' AS lastname,
        (
            (
                (
                    (COALESCE(prmc.outlet_no, ''::character varying))::text || ('-'::character varying)::text
                ) || (COALESCE(otlt.name, ''::character varying))::text
            )
        )::character varying AS customername,
        'Malaysia' AS country,
        prmc.region AS state,
        prmc.chain AS storereference,
        (upper((prmc.channel)::text))::character varying AS storetype,
        'MT' AS channel,
        otlt.chain_no AS salesgroup,
        'Malaysia' AS prod_hier_l1,
        null as prod_hier_l2,
        sku.category AS prod_hier_l3,
        null as prod_hier_l4,
        sku.brand AS prod_hier_l5,
        null as prod_hier_l6,
        null as prod_hier_l7,
        null as prod_hier_l8,
        null as prod_hier_l9,
        (wt.weight)::double precision AS kpi_chnl_wt,
        CASE
            WHEN (
                upper((prmc.promo_comp_on_time_in_full)::text) = ('YES'::character varying)::text
            ) THEN 'YES'::character varying
            WHEN (
                (
                    upper((prmc.promo_comp_on_time_in_full)::text) = ('NO'::character varying)::text
                )
                AND (
                    upper((prmc.promo_comp_successfully_set_up)::text) = ('YES'::character varying)::text
                )
            ) THEN 'YES'::character varying
            ELSE 'NO'::character varying
        END AS "y/n_flag",
        CASE
            WHEN (
                (
                    upper((prmc.promo_comp_on_time_in_full)::text) = ('NO'::character varying)::text
                )
                AND (
                    upper((prmc.promo_comp_successfully_set_up)::text) = ('YES'::character varying)::text
                )
            ) THEN 'Y'::character varying
            ELSE 'N'::character varying
        END AS posm_execution_flag,
        'Y' AS priority_store_flag,
        (
            (
                (
                    (
                        (
                            ('Is '::character varying)::text || (COALESCE(prmc.activation, ''::character varying))::text
                        ) || (' promotion for '::character varying)::text
                    ) || (COALESCE(sku.brand, ''::character varying))::text
                ) || (' being run?'::character varying)::text
            )
        )::character varying(255) AS questiontext,
        null as ques_desc,
        null as value,
        null as mkt_share,
        prmc.non_compliance_reason AS rej_reason
    FROM
        (
            SELECT
                edw_vw_my_ps_promocomp.outlet_no,
                edw_vw_my_ps_promocomp.outlet,
                edw_vw_my_ps_promocomp.region,
                edw_vw_my_ps_promocomp.chain,
                edw_vw_my_ps_promocomp.channel,
                edw_vw_my_ps_promocomp.category,
                edw_vw_my_ps_promocomp.brand,
                edw_vw_my_ps_promocomp.yearmo,
                "max"(
                    to_date(
                        (edw_vw_my_ps_promocomp.date)::timestamp without time zone
                    )
                ) AS visit_datetime
            FROM edw_vw_my_ps_promocomp
            GROUP BY edw_vw_my_ps_promocomp.outlet_no,
                edw_vw_my_ps_promocomp.outlet,
                edw_vw_my_ps_promocomp.region,
                edw_vw_my_ps_promocomp.chain,
                edw_vw_my_ps_promocomp.channel,
                edw_vw_my_ps_promocomp.category,
                edw_vw_my_ps_promocomp.brand,
                edw_vw_my_ps_promocomp.yearmo
        ) prm,
        edw_vw_my_ps_promocomp prmc
        LEFT JOIN
        (
            SELECT DISTINCT edw_vw_my_ps_sku_mst.category,
                edw_vw_my_ps_sku_mst.brand
            FROM edw_vw_my_ps_sku_mst
            WHERE (
                    upper((edw_vw_my_ps_sku_mst.manufacture)::text) = ('JOHNSON & JOHNSON'::character varying)::text
                )
        ) sku ON (((prmc.brand)::text = (sku.brand)::text))
        LEFT JOIN
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = ('PROMO COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_weights.channel)::text) = ('MT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_weights.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) wt ON
        (
            (
                rtrim(wt.retail_environment)::text = rtrim(prmc.channel)::text
            )
        )
        LEFT JOIN edw_vw_my_ps_outlet_mst otlt ON
        (
            ((otlt.outlet_no)::text = (prmc.outlet_no)::text)
        )
    WHERE
        (
            (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (
                                            to_date(
                                                (prm.visit_datetime)::timestamp without time zone
                                            ) = prmc.date
                                        )
                                        AND ((prm.outlet_no)::text = (prmc.outlet_no)::text)
                                    )
                                    AND ((prm.outlet)::text = (prmc.outlet)::text)
                                )
                                AND ((prm.region)::text = (prmc.region)::text)
                            )
                            AND ((prm.chain)::text = (prmc.chain)::text)
                        )
                        AND ((prm.channel)::text = (prmc.channel)::text)
                    )
                    AND ((prm.category)::text = (prmc.category)::text)
                )
                AND ((prm.brand)::text = (prmc.brand)::text)
            )
            AND
            (
                date_part(
                    year,
                    (prmc.date)::timestamp without time zone
                ) >= (
                    date_part(
                        year,
                        current_timestamp::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
),
merchandising_response_mt_msl as
(   
    SELECT 
        'Merchandising_Response' AS dataset,
        osa.outlet_no AS customerid,
        '' AS salespersonid,
        'true' AS mustcarryitem,
        null AS answerscore,
        CASE
            WHEN (sum(osa.answer) > 0) THEN 'true'::character varying
            ELSE 'false'::character varying
        END AS presence,
        null AS outofstock,
        'MSL Compliance' AS kpi,
        (
            (
                (
                    (
                        "left"(osa.mnth_id, 4) || ('-'::character varying)::text
                    ) || "right"(osa.mnth_id, 2)
                ) || ('-15'::character varying)::text
            )
        )::date AS scheduleddate,
        'completed' AS vst_status,
        ("left"(osa.mnth_id, 4))::character varying AS fisc_yr,
        (osa.mnth_id)::integer AS fisc_per,
        '' AS firstname,
        '' AS lastname,
        (
            (
                (
                    (COALESCE(osa.outlet_no, ''::character varying))::text || ('-'::character varying)::text
                ) || (COALESCE(otlt.name, ''::character varying))::text
            )
        )::character varying AS customername,
        'Malaysia' AS country,
        osa.region AS state,
        otlt.chain_no AS storereference,
        (upper((osa.channel_nm)::text))::character varying AS storetype,
        'MT' AS channel,
        osa.chain AS salesgroup,
        'Malaysia' AS prod_hier_l1,
        null AS prod_hier_l2,
        sku.category AS prod_hier_l3,
        sku.sub_catgory AS prod_hier_l4,
        sku.brand AS prod_hier_l5,
        sku.sub_brand AS prod_hier_l6,
        sku.packsize AS prod_hier_l7,
        null AS prod_hier_l8,
        COALESCE(osa.prod_nm, null) AS prod_hier_l9,
        (wt.weight)::double precision AS kpi_chnl_wt,
        null AS "y/n_flag",
        null AS posm_execution_flag,
        'Y' AS priority_store_flag,
        (
            (
                (
                    ('Is '::character varying)::text || (COALESCE(osa.prod_nm, ''::character varying))::text
                ) || (' listed in store?'::character varying)::text
            )
        )::character varying(255) AS questiontext,
        null AS ques_desc,
        null AS value,
        null AS mkt_share,
        null AS rej_reason
    FROM 
    (
        (
            (
                (
                    SELECT msl.channel_nm,
                        msl.ean,
                        msl.prod_nm,
                        msl.outlet_no,
                        msl.date,
                        "left"(
                            "replace"(
                                ((msl.date)::character varying)::text,
                                ('-'::character varying)::text,
                                (''::character varying)::text
                            ),
                            6
                        ) AS mnth_id,
                        msl.region,
                        msl.chain,
                        osa.answer AS answer_raw,
                        CASE
                            WHEN (
                                (
                                    upper((osa.answer)::text) = ('YES'::character varying)::text
                                )
                                OR (
                                    upper((osa.answer)::text) = ('NO'::character varying)::text
                                )
                            ) THEN 1
                            WHEN (
                                upper((osa.answer)::text) = ('NOT LISTED'::character varying)::text
                            ) THEN 0
                            ELSE 0
                        END AS answer
                    FROM (
                            (
                                SELECT msl.channel_nm,
                                    "max"((msl.ean)::text) AS ean,
                                    rtrim(msl.prod_nm) as prod_nm,
                                    rtrim(osa.outlet_no) as outlet_no,
                                    rtrim(osa.date) as date,
                                    rtrim(osa.region) as region,
                                    rtrim(osa.chain) as chain,
                                    rtrim(osa.channel) as channel
                                FROM 
                                    (
                                        (
                                            SELECT DISTINCT itg_mds_my_ps_msl.channel_nm,
                                                itg_mds_my_ps_msl.ean,
                                                itg_mds_my_ps_msl.prod_nm,
                                                itg_mds_my_ps_msl.crtd_dttm,
                                                itg_mds_my_ps_msl.valid_from,
                                                itg_mds_my_ps_msl.valid_to
                                            FROM itg_mds_my_ps_msl
                                            WHERE 
                                                (
                                                    COALESCE(
                                                        rtrim(upper((itg_mds_my_ps_msl.channel_nm)::text)),
                                                        ('NA'::character varying)::text
                                                    ) IN (
                                                        SELECT DISTINCT COALESCE(
                                                                rtrim(upper((edw_vw_my_ps_osa.channel)::text)),
                                                                ('NA'::character varying)::text
                                                            ) AS "coalesce"
                                                        FROM edw_vw_my_ps_osa
                                                    )
                                                )
                                        ) msl
                                        LEFT JOIN edw_vw_my_ps_osa osa ON 
                                        (
                                            (
                                                (
                                                    (
                                                        rtrim(upper((msl.channel_nm)::text)) = rtrim(upper((osa.channel)::text))
                                                    )
                                                    AND (
                                                        ((osa.date)::character varying)::text >= ((msl.valid_from)::character varying)::text
                                                    )
                                                )
                                                AND (
                                                    ((osa.date)::character varying)::text <= ((msl.valid_to)::character varying)::text
                                                )
                                            )
                                        )
                                    )
                                GROUP BY 
                                    msl.channel_nm,
                                    rtrim(msl.prod_nm),
                                    rtrim(osa.outlet_no),
                                    rtrim(osa.date),
                                    rtrim(osa.region),
                                    rtrim(osa.chain),
                                    rtrim(osa.channel)
                            ) msl
                            LEFT JOIN edw_vw_my_ps_osa osa ON 
                            (
                                (
                                    (
                                        (
                                            (msl.date = osa.date)
                                            AND ((msl.outlet_no)::text = (osa.outlet_no)::text)
                                        )
                                        AND (
                                            ltrim(msl.ean, ('0'::character varying)::text) = ltrim(
                                                (osa.product_barcode)::text,
                                                ('0'::character varying)::text
                                            )
                                        )
                                    )
                                    AND (
                                        rtrim(upper((msl.channel_nm)::text)) = rtrim((osa.channel))::text
                                    )
                                )
                            )
                        )
                ) osa
                LEFT JOIN 
                (
                    SELECT edw_vw_my_ps_sku_mst.sku_no,
                        edw_vw_my_ps_sku_mst.description,
                        edw_vw_my_ps_sku_mst.client,
                        edw_vw_my_ps_sku_mst.manufacture,
                        edw_vw_my_ps_sku_mst.category,
                        edw_vw_my_ps_sku_mst.brand,
                        edw_vw_my_ps_sku_mst.sub_catgory,
                        edw_vw_my_ps_sku_mst.sub_brand,
                        edw_vw_my_ps_sku_mst.packsize,
                        edw_vw_my_ps_sku_mst.other,
                        edw_vw_my_ps_sku_mst.product_barcode,
                        edw_vw_my_ps_sku_mst.list_price_fib,
                        edw_vw_my_ps_sku_mst.list_price_unit,
                        edw_vw_my_ps_sku_mst.rcp,
                        edw_vw_my_ps_sku_mst.packing_config
                    FROM edw_vw_my_ps_sku_mst
                    WHERE (
                            upper((edw_vw_my_ps_sku_mst.manufacture)::text) = ('JOHNSON & JOHNSON'::character varying)::text
                        )
                ) sku ON (
                    (
                        ltrim(
                            (sku.product_barcode)::text,
                            ('0'::character varying)::text
                        ) = ltrim(osa.ean, ('0'::character varying)::text)
                    )
                )
            )
            LEFT JOIN (
                SELECT edw_vw_ps_weights.market,
                    edw_vw_ps_weights.kpi,
                    edw_vw_ps_weights.channel,
                    edw_vw_ps_weights.retail_environment,
                    edw_vw_ps_weights.weight
                FROM {{ ref('aspedw_integration__edw_vw_ps_weights') }}
                WHERE (
                        (
                            (
                                upper((edw_vw_ps_weights.kpi)::text) = ('MSL COMPLIANCE'::character varying)::text
                            )
                            AND (
                                upper((edw_vw_ps_weights.channel)::text) = ('MT'::character varying)::text
                            )
                        )
                        AND (
                            upper((edw_vw_ps_weights.market)::text) = ('MALAYSIA'::character varying)::text
                        )
                    )
            ) wt ON (
                (
                    rtrim(upper((wt.retail_environment)::text)) = rtrim(upper((osa.channel_nm)::text)
                ))
            )
        )
        LEFT JOIN edw_vw_my_ps_outlet_mst otlt ON (((otlt.outlet_no)::text = (osa.outlet_no)::text))
    )
    WHERE (
        date_part(
            year,
            (osa.date)::timestamp without time zone
        ) >= (
            date_part(
                year,
                convert_timezone('UTC', current_timestamp())::timestamp without time zone
            ) - (2)::double precision
        )
    )
    GROUP BY 
        osa.outlet_no,
        osa.mnth_id,
        otlt.name,
        osa.region,
        otlt.chain_no,
        osa.channel_nm,
        osa.chain,
        sku.category,
        sku.sub_catgory,
        sku.brand,
        sku.sub_brand,
        sku.packsize,
        osa.prod_nm,
        (wt.weight)::double precision
),
merchandising_response_mt_oos as
(
    SELECT
        'Merchandising_Response' AS dataset,
        oos.outlet_no AS customerid,
        '' AS salespersonid,
        'true' AS mustcarryitem,
        null as answerscore,
        'true' AS presence,
        CASE
            WHEN (
                upper((oos.answer)::text) = ('YES'::character varying)::text
            ) THEN ''::character varying
            WHEN (
                upper((oos.answer)::text) = ('NO'::character varying)::text
            ) THEN 'true'::character varying
            ELSE NULL::character varying
        END AS outofstock,
        'OOS Compliance' AS kpi,
        oos.date AS scheduleddate,
        'completed' AS vst_status,
        ("left"(((oos.date)::character varying)::text, 4))::character varying AS fisc_yr,
        (
            (
                "left"(((oos.date)::character varying)::text, 4) || "substring"(((oos.date)::character varying)::text, 6, 2)
            )
        )::integer AS fisc_per,
        '' AS firstname,
        '' AS lastname,
        (
            (
                (
                    (COALESCE(oos.outlet_no, ''::character varying))::text || ('-'::character varying)::text
                ) || (COALESCE(otlt.name, ''::character varying))::text
            )
        )::character varying AS customername,
        'Malaysia' AS country,
        oos.region AS state,
        otlt.chain_no AS storereference,
        (upper((oos.channel)::text))::character varying AS storetype,
        'MT' AS channel,
        oos.chain AS salesgroup,
        'Malaysia' AS prod_hier_l1,
        null as prod_hier_l2,
        sku.category AS prod_hier_l3,
        sku.sub_catgory AS prod_hier_l4,
        sku.brand AS prod_hier_l5,
        sku.sub_brand AS prod_hier_l6,
        sku.packsize AS prod_hier_l7,
        null as prod_hier_l8,
        COALESCE(sku.description, msl.prod_nm) AS prod_hier_l9,
        (wt.weight)::double precision AS kpi_chnl_wt,
        null as "y/n_flag",
        null as posm_execution_flag,
        'Y' AS priority_store_flag,
        (
            (
                (
                    ('Is '::character varying)::text || (
                        COALESCE(oos.sku_description, ''::character varying)
                    )::text
                ) || (' listed in store?'::character varying)::text
            )
        )::character varying(255) AS questiontext,
        null as ques_desc,
        null as value,
        null as mkt_share,
        null as rej_reason
    FROM
        (
            SELECT itg_mds_my_ps_msl.channel_nm,
                itg_mds_my_ps_msl.ean,
                itg_mds_my_ps_msl.prod_nm,
                itg_mds_my_ps_msl.crtd_dttm,
                itg_mds_my_ps_msl.valid_from,
                itg_mds_my_ps_msl.valid_to
            FROM itg_mds_my_ps_msl
            WHERE
                (
                    COALESCE(
                        itg_mds_my_ps_msl.channel_nm,
                        'NA'::character varying
                    ) IN (
                        SELECT DISTINCT COALESCE(
                                edw_vw_my_ps_osa.channel,
                                'NA'::character varying
                            ) AS "coalesce"
                        FROM edw_vw_my_ps_osa
                    )
                )
        ) msl,
        edw_vw_my_ps_osa oos
        LEFT JOIN
        (
            SELECT edw_vw_my_ps_sku_mst.sku_no,
                edw_vw_my_ps_sku_mst.description,
                edw_vw_my_ps_sku_mst.client,
                edw_vw_my_ps_sku_mst.manufacture,
                edw_vw_my_ps_sku_mst.category,
                edw_vw_my_ps_sku_mst.brand,
                edw_vw_my_ps_sku_mst.sub_catgory,
                edw_vw_my_ps_sku_mst.sub_brand,
                edw_vw_my_ps_sku_mst.packsize,
                edw_vw_my_ps_sku_mst.other,
                edw_vw_my_ps_sku_mst.product_barcode,
                edw_vw_my_ps_sku_mst.list_price_fib,
                edw_vw_my_ps_sku_mst.list_price_unit,
                edw_vw_my_ps_sku_mst.rcp,
                edw_vw_my_ps_sku_mst.packing_config
            FROM edw_vw_my_ps_sku_mst
            WHERE (
                    upper((edw_vw_my_ps_sku_mst.manufacture)::text) = ('JOHNSON & JOHNSON'::character varying)::text
                )
        ) sku ON (sku.product_barcode)::text = (oos.product_barcode)::text
        LEFT JOIN
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = ('OSA COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_weights.channel)::text) = ('MT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_weights.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) wt ON
        (
            (
               rtrim( upper((wt.retail_environment)::text)) = rtrim(upper((oos.channel)::text))
            )
        )
        LEFT JOIN edw_vw_my_ps_outlet_mst otlt ON (((otlt.outlet_no)::text = (oos.outlet_no)::text))
    WHERE
        (
            (
                (
                    (
                        upper((msl.channel_nm)::text) = upper((oos.channel)::text)
                    )
                    AND (
                        ltrim((msl.ean)::text, ('0'::character varying)::text) = ltrim(
                            (oos.product_barcode)::text,
                            ('0'::character varying)::text
                        )
                    )
                )
                AND (
                    date_part(
                        year,
                        (oos.date)::timestamp without time zone
                    ) >= (
                        date_part(
                            year,
                            current_timestamp::timestamp without time zone
                        ) - (2)::double precision
                    )
                )
            )
            AND (
                upper((oos.answer)::text) <> upper(('Not Listed'::character varying)::text)
            )
        )
),
survey_response_mt_num as
(
    SELECT
        'Survey_Response' AS dataset,
        sos.outlet_no AS customerid,
        '' AS salespersonid,
        null as mustcarryitem,
        null as answerscore,
        null as presence,
        null as outofstock,
        'Share of Shelf' AS kpi,
        sos.date AS scheduleddate,
        'completed' AS vst_status,
        ("left"(((sos.date)::character varying)::text, 4))::character varying AS fisc_yr,
        (
            (
                "left"(((sos.date)::character varying)::text, 4) || "substring"(((sos.date)::character varying)::text, 6, 2)
            )
        )::integer AS fisc_per,
        '' AS firstname,
        '' AS lastname,
        (
            (
                (
                    (COALESCE(sos.outlet_no, ''::character varying))::text || ('-'::character varying)::text
                ) || (COALESCE(otlt.name, ''::character varying))::text
            )
        )::character varying AS customername,
        'Malaysia' AS country,
        sos.region AS state,
        otlt.chain_no AS storereference,
        (upper((sos.channel)::text))::character varying AS storetype,
        'MT' AS channel,
        sos.chain AS salesgroup,
        'Malaysia' AS prod_hier_l1,
        'JOHNSON & JOHNSON' AS prod_hier_l2,
        sku.category AS prod_hier_l3,
        null as prod_hier_l4,
        sku.brand AS prod_hier_l5,
        null as prod_hier_l6,
        null as prod_hier_l7,
        null as prod_hier_l8,
        null as prod_hier_l9,
        (wt.weight)::double precision AS kpi_chnl_wt,
        null as "y/n_flag",
        null as posm_execution_flag,
        'Y' AS priority_store_flag,
        (
            (
                (
                    (
                        'What is the Share of Space for '::character varying
                    )::text || (
                        COALESCE(sos.sku_description, ''::character varying)
                    )::text
                ) || ('?'::character varying)::text
            )
        )::character varying(255) AS questiontext,
        'NUMERATOR' AS ques_desc,
        ((sos.answer)::numeric(18, 0))::numeric(20, 2) AS value,
        trgt.value AS mkt_share,
        null as rej_reason
    FROM
        (
            SELECT DISTINCT upper((edw_vw_my_ps_sku_mst.manufacture)::text) AS manufacture,
                edw_vw_my_ps_sku_mst.category,
                edw_vw_my_ps_sku_mst.brand
            FROM edw_vw_my_ps_sku_mst
            WHERE (
                    upper((edw_vw_my_ps_sku_mst.manufacture)::text) = ('JOHNSON & JOHNSON'::character varying)::text
                )
        ) sku,
        edw_vw_my_ps_sos sos
        LEFT JOIN
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE
                (
                    (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = ('SOS COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_weights.channel)::text) = ('MT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_weights.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) wt ON
        (
            (
                rtrim(upper((wt.retail_environment)::text)) = rtrim(upper((sos.channel)::text))
            )
        )
        LEFT JOIN
        (
            SELECT edw_vw_ps_targets.market,
                edw_vw_ps_targets.kpi,
                edw_vw_ps_targets.channel,
                edw_vw_ps_targets.retail_environment,
                edw_vw_ps_targets.attribute_1,
                edw_vw_ps_targets.attribute_2,
                edw_vw_ps_targets.value
            FROM edw_vw_ps_targets
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_targets.kpi)::text) = ('SOS COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_targets.channel)::text) = ('MT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_targets.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) trgt ON
        (
            (
                (
                    (
                        upper((trgt.retail_environment)::text) = upper((sos.channel)::text)
                    )
                    AND (
                        upper((trgt.attribute_2)::text) = upper((sos.brand)::text)
                    )
                )
                AND (
                    upper((trgt.attribute_1)::text) = upper((sos.category)::text)
                )
            )
        )
        LEFT JOIN edw_vw_my_ps_outlet_mst otlt ON (((otlt.outlet_no)::text = (sos.outlet_no)::text))
    WHERE
        (
            (
                upper((sos.brand)::text) = upper((sku.brand)::text)
            )
            AND (
                date_part(
                    year,
                    (sos.date)::timestamp without time zone
                ) >= (
                    date_part(
                        year,
                        current_timestamp::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
),
survey_response_mt_den as
(
    SELECT
        'Survey_Response' AS dataset,
        sos.outlet_no AS customerid,
        '' AS salespersonid,
        null as mustcarryitem,
        null as answerscore,
        null as presence,
        null as outofstock,
        'Share of Shelf' AS kpi,
        sos.date AS scheduleddate,
        'completed' AS vst_status,
        ("left"(((sos.date)::character varying)::text, 4))::character varying AS fisc_yr,
        (
            (
                "left"(((sos.date)::character varying)::text, 4) || "substring"(((sos.date)::character varying)::text, 6, 2)
            )
        )::integer AS fisc_per,
        '' AS firstname,
        '' AS lastname,
        (
            (
                (
                    (COALESCE(sos.outlet_no, ''::character varying))::text || ('-'::character varying)::text
                ) || (COALESCE(otlt.name, ''::character varying))::text
            )
        )::character varying AS customername,
        'Malaysia' AS country,
        sos.region AS state,
        sos.chain AS storereference,
        (upper((sos.channel)::text))::character varying AS storetype,
        'MT' AS channel,
        otlt.chain_no AS salesgroup,
        'Malaysia' AS prod_hier_l1,
        (sku.manufacture)::character varying AS prod_hier_l2,
        sku.category AS prod_hier_l3,
        null as prod_hier_l4,
        sku.brand AS prod_hier_l5,
        null as prod_hier_l6,
        null as prod_hier_l7,
        null as prod_hier_l8,
        null as prod_hier_l9,
        (wt.weight)::double precision AS kpi_chnl_wt,
        (
            row_number() OVER(
                PARTITION BY sos.date,
                sos.outlet_no,
                sos.category,
                sos.brand
            order by null)
        )::character varying AS "y/n_flag",
        null as posm_execution_flag,
        'Y' AS priority_store_flag,
        (
            (
                (
                    (
                        'What is the total shelf facing of the '::character varying
                    )::text || (COALESCE(sos.category, ''::character varying))::text
                ) || (' category ?'::character varying)::text
            )
        )::character varying(255) AS questiontext,
        'DENOMINATOR' AS ques_desc,
        sos.total_facing AS value,
        trgt.value AS mkt_share,
        null as rej_reason
    FROM
        (
            SELECT DISTINCT upper((edw_vw_my_ps_sku_mst.manufacture)::text) AS manufacture,
                edw_vw_my_ps_sku_mst.category,
                edw_vw_my_ps_sku_mst.brand
            FROM edw_vw_my_ps_sku_mst
            WHERE (
                    upper((edw_vw_my_ps_sku_mst.manufacture)::text) = ('JOHNSON & JOHNSON'::character varying)::text
                )
        ) sku,
        (
            SELECT DISTINCT sos.yearmo,
                sos.date,
                sos.channel,
                sos.chain,
                sos.region,
                sos.outlet,
                sos.outlet_no,
                sos.category,
                sos.brand,
                total_brnd.visit_date,
                total_brnd.store,
                total_brnd.tot_category,
                total_brnd.total_facing
            FROM edw_vw_my_ps_sos sos,
                (
                    SELECT edw_vw_my_ps_sos.date AS visit_date,
                        edw_vw_my_ps_sos.outlet_no AS store,
                        edw_vw_my_ps_sos.category AS tot_category,
                        sum(
                            ((edw_vw_my_ps_sos.answer)::numeric(18, 0))::numeric(20, 2)
                        ) AS total_facing
                    FROM edw_vw_my_ps_sos
                    GROUP BY edw_vw_my_ps_sos.date,
                        edw_vw_my_ps_sos.outlet_no,
                        edw_vw_my_ps_sos.category
                ) total_brnd
            WHERE
                (
                    (
                        (sos.date = total_brnd.visit_date)
                        AND ((sos.outlet_no)::text = (total_brnd.store)::text)
                    )
                    AND (
                        (sos.category)::text = (total_brnd.tot_category)::text
                    )
                )
        ) sos
        LEFT JOIN
        (
            SELECT edw_vw_ps_weights.market,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.weight
            FROM edw_vw_ps_weights
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = ('SOS COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_weights.channel)::text) = ('MT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_weights.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) wt ON rtrim(upper((wt.retail_environment)::text)) = rtrim(upper((sos.channel)::text))
        LEFT JOIN
        (
            SELECT edw_vw_ps_targets.market,
                edw_vw_ps_targets.kpi,
                edw_vw_ps_targets.channel,
                edw_vw_ps_targets.retail_environment,
                edw_vw_ps_targets.attribute_1,
                edw_vw_ps_targets.attribute_2,
                edw_vw_ps_targets.value
            FROM edw_vw_ps_targets
            WHERE
                (
                    (
                        (
                            upper((edw_vw_ps_targets.kpi)::text) = ('SOS COMPLIANCE'::character varying)::text
                        )
                        AND (
                            upper((edw_vw_ps_targets.channel)::text) = ('MT'::character varying)::text
                        )
                    )
                    AND (
                        upper((edw_vw_ps_targets.market)::text) = ('MALAYSIA'::character varying)::text
                    )
                )
        ) trgt ON
        (
            (
                (
                    (
                        upper((trgt.retail_environment)::text) = upper((sos.channel)::text)
                    )
                    AND (
                        upper((trgt.attribute_2)::text) = upper((sos.brand)::text)
                    )
                )
                AND (
                    upper((trgt.attribute_1)::text) = upper((sos.category)::text)
                )
            )
        )
        LEFT JOIN edw_vw_my_ps_outlet_mst otlt ON (((otlt.outlet_no)::text = (sos.outlet_no)::text))
    WHERE
        (
            (
                upper((sku.brand)::text) = upper((sos.brand)::text)
            )
            AND (
                date_part(
                    year,
                    (sos.date)::timestamp without time zone
                ) >= (
                    date_part(
                        year,
                        current_timestamp::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
),
final as
(
    select * from merchandising_response_gt_msl
    union all
    select * from survey_response_promo
    union all
    select * from merchandising_response_mt_msl
    union all
    select * from merchandising_response_mt_oos
    union all
    select * from survey_response_mt_num
    union all
    select * from survey_response_mt_den
)
select * from final