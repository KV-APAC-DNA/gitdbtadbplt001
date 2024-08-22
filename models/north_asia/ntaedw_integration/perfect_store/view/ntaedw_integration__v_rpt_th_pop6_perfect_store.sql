WITH edw_vw_pop6_visits_sku_audits
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_visits_sku_audits') }}
    ),
edw_vw_pop6_th_visits_prod_attribute_audits
AS (
    SELECT *
    FROM {{ ref('ntaedw_integration__edw_vw_pop6_th_visits_prod_attribute_audits') }}
    ),
edw_vw_pop6_store
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_store') }}
    ),
edw_vw_pop6_products
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_products') }}
    ),
edw_vw_pop6_salesperson
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_salesperson') }}
    ),
edw_vw_pop6_visits_display
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_visits_display') }}
    ),
edw_vw_pop6_visits_prod_attribute_audits
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_visits_prod_attribute_audits') }}
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
edw_vw_pop6_visits_rir_data
AS (
    SELECT * FROM {{ ref('aspedw_integration__edw_vw_pop6_visits_rir_data') }}
   ),
itg_mds_th_ps_handeye_level
AS (
    SELECT * FROM {{ ref('thaitg_integration__itg_mds_th_ps_handeye_level') }}
   ), 
ct1
AS (
    SELECT 'Merchandising_Response' AS dataset,
        NULL AS merchandisingresponseid,
        NULL AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL AS mrch_resp_status,
        NULL AS mastersurveyid,
        NULL AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL AS valuekey,
        vst.response AS value,
        vst.sku_id AS productid,
        'true' AS mustcarryitem,
        CASE 
            WHEN (
                    (upper((vst.response)::TEXT) = ('YES'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ((1)::CHARACTER VARYING)::TEXT)
                    )
                THEN 'true'::CHARACTER VARYING
            ELSE 'false'::CHARACTER VARYING
            END AS presence,
        NULL AS outofstock,
        'MSL Compliance' AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        CASE 
            WHEN (vst.cancelled_visit = 0)
                THEN 'completed'::CHARACTER VARYING
            ELSE 'cancelled'::CHARACTER VARYING
            END AS vst_status,
        NULL AS fisc_yr,
        NULL AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS STATE,
        NULL AS county,
        NULL AS district,
        NULL AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL AS soldtoparty,
        prd.sku_english AS productname,
        prd.barcode AS eannumber,
        NULL AS matl_num,
        prd.country_l1 AS prod_hier_l1,
        prd.regional_franchise_l2 AS prod_hier_l2,
        prd.franchise_l3 AS prod_hier_l3,
        prd.brand_l4 AS prod_hier_l4,
        prd.sub_category_l5 AS prod_hier_l5,
        prd.platform_l6 AS prod_hier_l6,
        prd.variance_l7 AS prod_hier_l7,
        prd.pack_size_l8 AS prod_hier_l8,
        NULL AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        NULL AS mkt_share,
        NULL AS ques_desc,
        NULL AS "y/n_flag",
        NULL AS rej_reason,
        NULL AS response,
        NULL AS response_score,
        NULL AS acc_rej_reason,
        NULL as actual_value,
        NULL as ref_value,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration
    FROM (
        (
            (
                (
                    (
                        SELECT edw_vw_pop6_visits_sku_audits.cntry_cd,
                            edw_vw_pop6_visits_sku_audits.visit_id,
                            edw_vw_pop6_visits_sku_audits.audit_form_id,
                            edw_vw_pop6_visits_sku_audits.audit_form,
                            edw_vw_pop6_visits_sku_audits.section_id,
                            edw_vw_pop6_visits_sku_audits.section,
                            edw_vw_pop6_visits_sku_audits.field_id,
                            edw_vw_pop6_visits_sku_audits.field_code,
                            edw_vw_pop6_visits_sku_audits.field_label,
                            edw_vw_pop6_visits_sku_audits.field_type,
                            edw_vw_pop6_visits_sku_audits.dependent_on_field_id,
                            edw_vw_pop6_visits_sku_audits.sku_id,
                            edw_vw_pop6_visits_sku_audits.sku,
                            edw_vw_pop6_visits_sku_audits.response,
                            edw_vw_pop6_visits_sku_audits.visit_date,
                            edw_vw_pop6_visits_sku_audits.check_in_datetime,
                            edw_vw_pop6_visits_sku_audits.check_out_datetime,
                            edw_vw_pop6_visits_sku_audits.popdb_id,
                            edw_vw_pop6_visits_sku_audits.pop_code,
                            edw_vw_pop6_visits_sku_audits.pop_name,
                            edw_vw_pop6_visits_sku_audits.address,
                            edw_vw_pop6_visits_sku_audits.check_in_longitude,
                            edw_vw_pop6_visits_sku_audits.check_in_latitude,
                            edw_vw_pop6_visits_sku_audits.check_out_longitude,
                            edw_vw_pop6_visits_sku_audits.check_out_latitude,
                            edw_vw_pop6_visits_sku_audits.check_in_photo,
                            edw_vw_pop6_visits_sku_audits.check_out_photo,
                            edw_vw_pop6_visits_sku_audits.username,
                            edw_vw_pop6_visits_sku_audits.user_full_name,
                            edw_vw_pop6_visits_sku_audits.superior_username,
                            edw_vw_pop6_visits_sku_audits.superior_name,
                            edw_vw_pop6_visits_sku_audits.planned_visit,
                            edw_vw_pop6_visits_sku_audits.cancelled_visit,
                            edw_vw_pop6_visits_sku_audits.cancellation_reason,
                            edw_vw_pop6_visits_sku_audits.cancellation_note
                        FROM edw_vw_pop6_visits_sku_audits
                        WHERE (upper((edw_vw_pop6_visits_sku_audits.cntry_cd)::TEXT) = ('TH'::CHARACTER VARYING)::TEXT)
                        ) AS vst LEFT JOIN edw_vw_pop6_store AS pop ON (
                            (
                                ((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT)
                                AND ((pop.cntry_cd)::TEXT = ('TH'::CHARACTER VARYING)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    -- SELECT DISTINCT edw_vw_ps_weights.kpi,
                    --     edw_vw_ps_weights.weight,
                    --     CASE 
                    --         WHEN ((edw_vw_ps_weights.retail_environment)::TEXT = ('GT Big MM'::CHARACTER VARYING)::TEXT)
                    --             THEN 'GT BigMinimart'::CHARACTER VARYING
                    --         ELSE edw_vw_ps_weights.retail_environment
                    --         END AS retail_environment,
                    --     CASE 
                    --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('GT'::CHARACTER VARYING)::TEXT)
                    --             THEN 'General Trade'::CHARACTER VARYING
                    --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('MT'::CHARACTER VARYING)::TEXT)
                    --             THEN 'Modern Trade'::CHARACTER VARYING
                    --         ELSE edw_vw_ps_weights.channel
                    --         END AS channel
                    -- FROM edw_vw_ps_weights
                    -- WHERE (
                    --         (upper((edw_vw_ps_weights.kpi)::TEXT) = ('MSL COMPLIANCE'::CHARACTER VARYING)::TEXT)
                    --         AND (upper((edw_vw_ps_weights.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
                    --         )
                    SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                        edw_vw_ps_weights.kpi,
                        edw_vw_ps_weights.weight,
                        edw_vw_ps_weights.channel
                    FROM edw_vw_ps_weights
                    WHERE upper(edw_vw_ps_weights.kpi::text) = 'MSL COMPLIANCE'::character varying::text
                        AND upper(edw_vw_ps_weights.market::text) = 'THAILAND'::character varying::text
                    ) AS kpi_wt ON (
                        (
                            (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                            AND (upper((kpi_wt.channel)::TEXT) = upper((pop.channel)::TEXT))
                            )
                        )
                ) LEFT JOIN edw_vw_pop6_salesperson AS usr ON (
                    (
                        ((vst.username)::TEXT = (usr.username)::TEXT)
                        AND ((usr.cntry_cd)::TEXT = ('TH'::CHARACTER VARYING)::TEXT)
                        )
                    )
            ) LEFT JOIN edw_vw_pop6_products AS prd ON (
                (
                    ((vst.sku_id)::TEXT = (prd.productdb_id)::TEXT)
                    AND ((prd.cntry_cd)::TEXT = ('TH'::CHARACTER VARYING)::TEXT)
                    )
                )
        )
    WHERE (
            (upper((vst.field_code)::TEXT) = ('PS_MSL'::CHARACTER VARYING)::TEXT)
            AND date_part('year', vst.visit_date) >= (date_part('year', '2024-02-09 06:03:00.8736'::TIMESTAMP) - 2)
            )
    
    UNION ALL
    
    SELECT 'Merchandising_Response' AS dataset,
        NULL AS merchandisingresponseid,
        NULL AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL AS mrch_resp_status,
        NULL AS mastersurveyid,
        NULL AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL AS valuekey,
        vst.response AS value,
        vst.sku_id AS productid,
        'true' AS mustcarryitem,
        'true' AS presence,
        CASE 
            WHEN (
                    (upper((vst.response)::TEXT) = ('NO'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ((0)::CHARACTER VARYING)::TEXT)
                    )
                THEN (
                        coalesce(CASE 
                                WHEN (trim((vst_oos.response)::TEXT) = (''::CHARACTER VARYING)::TEXT)
                                    THEN (NULL::CHARACTER VARYING)::TEXT
                                ELSE trim(('TRUE'::CHARACTER VARYING)::TEXT)
                                END, ('other'::CHARACTER VARYING)::TEXT)
                        )::CHARACTER VARYING
            ELSE ''::CHARACTER VARYING
            END AS outofstock,
        'OOS Compliance' AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        CASE 
            WHEN (vst.cancelled_visit = 0)
                THEN 'completed'::CHARACTER VARYING
            ELSE 'cancelled'::CHARACTER VARYING
            END AS vst_status,
        NULL AS fisc_yr,
        NULL AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS STATE,
        NULL AS county,
        NULL AS district,
        NULL AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL AS soldtoparty,
        prd.sku_english AS productname,
        prd.barcode AS eannumber,
        NULL AS matl_num,
        prd.country_l1 AS prod_hier_l1,
        prd.regional_franchise_l2 AS prod_hier_l2,
        prd.franchise_l3 AS prod_hier_l3,
        prd.brand_l4 AS prod_hier_l4,
        prd.sub_category_l5 AS prod_hier_l5,
        prd.platform_l6 AS prod_hier_l6,
        prd.variance_l7 AS prod_hier_l7,
        prd.pack_size_l8 AS prod_hier_l8,
        NULL AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        NULL AS mkt_share,
        NULL AS ques_desc,
        NULL AS "y/n_flag",
        (trim((vst_oos.response)::TEXT))::CHARACTER VARYING AS rej_reason,
        NULL AS response,
        NULL AS response_score,
        NULL AS acc_rej_reason,
        NULL as actual_value,
        NULL as ref_value,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration
    FROM (
        (
            (
                (
                    (
                        (
                            SELECT edw_vw_pop6_visits_sku_audits.cntry_cd,
                                edw_vw_pop6_visits_sku_audits.visit_id,
                                edw_vw_pop6_visits_sku_audits.audit_form_id,
                                edw_vw_pop6_visits_sku_audits.audit_form,
                                edw_vw_pop6_visits_sku_audits.section_id,
                                edw_vw_pop6_visits_sku_audits.section,
                                edw_vw_pop6_visits_sku_audits.field_id,
                                edw_vw_pop6_visits_sku_audits.field_code,
                                edw_vw_pop6_visits_sku_audits.field_label,
                                edw_vw_pop6_visits_sku_audits.field_type,
                                edw_vw_pop6_visits_sku_audits.dependent_on_field_id,
                                edw_vw_pop6_visits_sku_audits.sku_id,
                                edw_vw_pop6_visits_sku_audits.sku,
                                edw_vw_pop6_visits_sku_audits.response,
                                edw_vw_pop6_visits_sku_audits.visit_date,
                                edw_vw_pop6_visits_sku_audits.check_in_datetime,
                                edw_vw_pop6_visits_sku_audits.check_out_datetime,
                                edw_vw_pop6_visits_sku_audits.popdb_id,
                                edw_vw_pop6_visits_sku_audits.pop_code,
                                edw_vw_pop6_visits_sku_audits.pop_name,
                                edw_vw_pop6_visits_sku_audits.address,
                                edw_vw_pop6_visits_sku_audits.check_in_longitude,
                                edw_vw_pop6_visits_sku_audits.check_in_latitude,
                                edw_vw_pop6_visits_sku_audits.check_out_longitude,
                                edw_vw_pop6_visits_sku_audits.check_out_latitude,
                                edw_vw_pop6_visits_sku_audits.check_in_photo,
                                edw_vw_pop6_visits_sku_audits.check_out_photo,
                                edw_vw_pop6_visits_sku_audits.username,
                                edw_vw_pop6_visits_sku_audits.user_full_name,
                                edw_vw_pop6_visits_sku_audits.superior_username,
                                edw_vw_pop6_visits_sku_audits.superior_name,
                                edw_vw_pop6_visits_sku_audits.planned_visit,
                                edw_vw_pop6_visits_sku_audits.cancelled_visit,
                                edw_vw_pop6_visits_sku_audits.cancellation_reason,
                                edw_vw_pop6_visits_sku_audits.cancellation_note
                            FROM edw_vw_pop6_visits_sku_audits
                            WHERE (upper((edw_vw_pop6_visits_sku_audits.cntry_cd)::TEXT) = ('TH'::CHARACTER VARYING)::TEXT)
                            ) AS vst LEFT JOIN edw_vw_pop6_store AS pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                        ) LEFT JOIN (
                        -- SELECT DISTINCT edw_vw_ps_weights.kpi,
                        --     edw_vw_ps_weights.weight,
                        --     CASE 
                        --         WHEN ((edw_vw_ps_weights.retail_environment)::TEXT = ('GT Big MM'::CHARACTER VARYING)::TEXT)
                        --             THEN 'GT BigMinimart'::CHARACTER VARYING
                        --         ELSE edw_vw_ps_weights.retail_environment
                        --         END AS retail_environment,
                        --     CASE 
                        --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('GT'::CHARACTER VARYING)::TEXT)
                        --             THEN 'General Trade'::CHARACTER VARYING
                        --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('MT'::CHARACTER VARYING)::TEXT)
                        --             THEN 'Modern Trade'::CHARACTER VARYING
                        --         ELSE edw_vw_ps_weights.channel
                        --         END AS channel
                        -- FROM edw_vw_ps_weights
                        -- WHERE (
                        --         (upper((edw_vw_ps_weights.kpi)::TEXT) = ('OSA COMPLIANCE'::CHARACTER VARYING)::TEXT)
                        --         AND (upper((edw_vw_ps_weights.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
                        --         )
                        SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                            edw_vw_ps_weights.kpi,
                            edw_vw_ps_weights.weight,
                            edw_vw_ps_weights.channel
                        FROM edw_vw_ps_weights
                        WHERE upper(edw_vw_ps_weights.kpi::text) = 'OSA COMPLIANCE'::character varying::text
                            AND upper(edw_vw_ps_weights.market::text) = 'THAILAND'::character varying::text
                        ) AS kpi_wt ON (
                            (
                                (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                                AND (upper((kpi_wt.channel)::TEXT) = upper((pop.channel)::TEXT))
                                )
                            )
                    ) LEFT JOIN edw_vw_pop6_salesperson AS usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                ) LEFT JOIN edw_vw_pop6_products AS prd ON (((vst.sku_id)::TEXT = (prd.productdb_id)::TEXT))
            ) LEFT JOIN (
            SELECT edw_vw_pop6_visits_sku_audits.cntry_cd,
                edw_vw_pop6_visits_sku_audits.visit_id,
                edw_vw_pop6_visits_sku_audits.audit_form_id,
                edw_vw_pop6_visits_sku_audits.audit_form,
                edw_vw_pop6_visits_sku_audits.section_id,
                edw_vw_pop6_visits_sku_audits.section,
                edw_vw_pop6_visits_sku_audits.field_id,
                edw_vw_pop6_visits_sku_audits.field_code,
                edw_vw_pop6_visits_sku_audits.field_label,
                edw_vw_pop6_visits_sku_audits.field_type,
                edw_vw_pop6_visits_sku_audits.dependent_on_field_id,
                edw_vw_pop6_visits_sku_audits.sku_id,
                edw_vw_pop6_visits_sku_audits.sku,
                edw_vw_pop6_visits_sku_audits.response,
                edw_vw_pop6_visits_sku_audits.visit_date,
                edw_vw_pop6_visits_sku_audits.check_in_datetime,
                edw_vw_pop6_visits_sku_audits.check_out_datetime,
                edw_vw_pop6_visits_sku_audits.popdb_id,
                edw_vw_pop6_visits_sku_audits.pop_code,
                edw_vw_pop6_visits_sku_audits.pop_name,
                edw_vw_pop6_visits_sku_audits.address,
                edw_vw_pop6_visits_sku_audits.check_in_longitude,
                edw_vw_pop6_visits_sku_audits.check_in_latitude,
                edw_vw_pop6_visits_sku_audits.check_out_longitude,
                edw_vw_pop6_visits_sku_audits.check_out_latitude,
                edw_vw_pop6_visits_sku_audits.check_in_photo,
                edw_vw_pop6_visits_sku_audits.check_out_photo,
                edw_vw_pop6_visits_sku_audits.username,
                edw_vw_pop6_visits_sku_audits.user_full_name,
                edw_vw_pop6_visits_sku_audits.superior_username,
                edw_vw_pop6_visits_sku_audits.superior_name,
                edw_vw_pop6_visits_sku_audits.planned_visit,
                edw_vw_pop6_visits_sku_audits.cancelled_visit,
                edw_vw_pop6_visits_sku_audits.cancellation_reason,
                edw_vw_pop6_visits_sku_audits.cancellation_note
            FROM edw_vw_pop6_visits_sku_audits
            WHERE (upper((edw_vw_pop6_visits_sku_audits.field_code)::TEXT) = ('PS_MSL_OOS_REASON'::CHARACTER VARYING)::TEXT)
            ) AS vst_oos ON (
                (
                    (
                        ((vst.visit_id)::TEXT = (vst_oos.visit_id)::TEXT)
                        AND ((vst.sku_id)::TEXT = (vst_oos.sku_id)::TEXT)
                        )
                    AND ((vst.field_id)::TEXT = (vst_oos.dependent_on_field_id)::TEXT)
                    )
                )
        )
    WHERE (
            (upper((vst.field_code)::TEXT) = ('PS_MSL_OOS'::CHARACTER VARYING)::TEXT)
            AND date_part('year', vst.visit_date) >= (date_part('year', '2024-02-09 06:03:00.8736'::TIMESTAMP) - 2)
            )
    ),
ct2
AS (
    SELECT 'Survey_Response' AS dataset,
        NULL AS merchandisingresponseid,
        NULL AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL AS mrch_resp_status,
        NULL AS mastersurveyid,
        NULL AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL AS valuekey,
        NULL AS value,
        NULL AS productid,
        NULL AS mustcarryitem,
        NULL AS presence,
        NULL AS outofstock,
        vst.kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        vst.vst_status,
        NULL AS fisc_yr,
        NULL AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS STATE,
        NULL AS county,
        NULL AS district,
        NULL AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL AS soldtoparty,
        NULL AS productname,
        NULL AS eannumber,
        NULL AS matl_num,
        NULL AS prod_hier_l1,
        NULL AS prod_hier_l2,
        NULL AS prod_hier_l3,
        NULL AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        NULL AS mkt_share,
        NULL AS ques_desc,
        CASE 
            WHEN (upper((rej.response)::TEXT) LIKE ('N.A%'::CHARACTER VARYING)::TEXT)
                THEN ' '::CHARACTER VARYING
            WHEN (
                    (upper((vst.response)::TEXT) = ('YES'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('1'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'YES'::CHARACTER VARYING
            WHEN (
                    (upper((vst.response)::TEXT) = ('NO'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('0'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'NO'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS "y/n_flag",
        CASE 
            WHEN (
                    (upper((vst.response)::TEXT) = ('YES'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('1'::CHARACTER VARYING)::TEXT)
                    )
                THEN ' '::CHARACTER VARYING
            WHEN (
                    (upper((vst.response)::TEXT) = ('NO'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('0'::CHARACTER VARYING)::TEXT)
                    )
                THEN rej.response
            ELSE vst.response
            END AS rej_reason,
        NULL AS response,
        NULL AS response_score,
        NULL AS acc_rej_reason,
        NULL as actual_value,
        NULL as ref_value,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration
    FROM (
        (
            (
                (
                    (
                        (
                            SELECT DISTINCT edw_vw_pop6_visits_display.cntry_cd,
                                edw_vw_pop6_visits_display.visit_id,
                                edw_vw_pop6_visits_display.field_code,
                                edw_vw_pop6_visits_display.field_label,
                                edw_vw_pop6_visits_display.field_id,
                                edw_vw_pop6_visits_display.field_type,
                                edw_vw_pop6_visits_display.dependent_on_field_id,
                                edw_vw_pop6_visits_display.response,
                                edw_vw_pop6_visits_display.visit_date,
                                edw_vw_pop6_visits_display.check_in_datetime,
                                edw_vw_pop6_visits_display.check_out_datetime,
                                edw_vw_pop6_visits_display.popdb_id,
                                edw_vw_pop6_visits_display.username,
                                edw_vw_pop6_visits_display.cancelled_visit,
                                edw_vw_pop6_visits_display.product_attribute,
                                edw_vw_pop6_visits_display.product_attribute_value,
                                CASE 
                                    WHEN (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE ('%PS_PROMO%'::CHARACTER VARYING)::TEXT)
                                        THEN 'Promo Compliance'::CHARACTER VARYING
                                    WHEN (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE ('%PS_DISPLAY%'::CHARACTER VARYING)::TEXT)
                                        THEN 'Display Compliance'::CHARACTER VARYING
                                    ELSE NULL::CHARACTER VARYING
                                    END AS kpi,
                                CASE 
                                    WHEN (edw_vw_pop6_visits_display.cancelled_visit = 0)
                                        THEN 'completed'::CHARACTER VARYING
                                    ELSE 'cancelled'::CHARACTER VARYING
                                    END AS vst_status
                            FROM edw_vw_pop6_visits_display
                            WHERE (
                                    (
                                        (upper((edw_vw_pop6_visits_display.cntry_cd)::TEXT) = ('TH'::CHARACTER VARYING)::TEXT)
                                        AND (
                                            (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE ('%PS_PROMO%'::CHARACTER VARYING)::TEXT)
                                            OR (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE ('%PS_DISPLAY%'::CHARACTER VARYING)::TEXT)
                                            )
                                        )
                                    AND ((edw_vw_pop6_visits_display.field_type)::TEXT = ('Yes_No'::CHARACTER VARYING)::TEXT)
                                    )
                            ) AS vst LEFT JOIN edw_vw_pop6_store AS pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                        ) LEFT JOIN edw_vw_pop6_salesperson AS usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                    ) LEFT JOIN (
                    SELECT DISTINCT NULL::CHARACTER VARYING AS brand_l4,
                        (upper((edw_vw_pop6_products.ps_category)::TEXT))::CHARACTER VARYING AS ps_category,
                        edw_vw_pop6_products.ps_segment,
                        (((upper((edw_vw_pop6_products.ps_category)::TEXT) || ('_'::CHARACTER VARYING)::TEXT) || (edw_vw_pop6_products.ps_segment)::TEXT))::CHARACTER VARYING AS ps_categorysegement
                    FROM edw_vw_pop6_products
                    
                    UNION
                    
                    SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                        NULL::CHARACTER VARYING AS ps_category,
                        NULL::CHARACTER VARYING AS ps_segment,
                        NULL::CHARACTER VARYING AS ps_categorysegement
                    FROM edw_vw_pop6_products
                    ) AS prd ON (
                        CASE 
                            WHEN ((vst.product_attribute)::TEXT = ('PS Category Segment'::CHARACTER VARYING)::TEXT)
                                THEN (upper((vst.product_attribute_value)::TEXT) = upper((prd.ps_categorysegement)::TEXT))
                            ELSE (upper(trim((vst.product_attribute_value)::TEXT)) = upper(trim((prd.brand_l4)::TEXT)))
                            END
                        )
                ) LEFT JOIN (
                SELECT edw_vw_pop6_visits_display.cntry_cd,
                    edw_vw_pop6_visits_display.visit_id,
                    edw_vw_pop6_visits_display.display_plan_id,
                    edw_vw_pop6_visits_display.display_type,
                    edw_vw_pop6_visits_display.display_code,
                    edw_vw_pop6_visits_display.display_name,
                    edw_vw_pop6_visits_display.start_date,
                    edw_vw_pop6_visits_display.end_date,
                    edw_vw_pop6_visits_display.checklist_method,
                    edw_vw_pop6_visits_display.display_number,
                    edw_vw_pop6_visits_display.product_attribute_id,
                    edw_vw_pop6_visits_display.product_attribute,
                    edw_vw_pop6_visits_display.product_attribute_value_id,
                    edw_vw_pop6_visits_display.product_attribute_value,
                    edw_vw_pop6_visits_display.comments,
                    edw_vw_pop6_visits_display.field_id,
                    edw_vw_pop6_visits_display.field_code,
                    edw_vw_pop6_visits_display.field_label,
                    edw_vw_pop6_visits_display.field_type,
                    edw_vw_pop6_visits_display.dependent_on_field_id,
                    edw_vw_pop6_visits_display.response,
                    edw_vw_pop6_visits_display.visit_date,
                    edw_vw_pop6_visits_display.check_in_datetime,
                    edw_vw_pop6_visits_display.check_out_datetime,
                    edw_vw_pop6_visits_display.popdb_id,
                    edw_vw_pop6_visits_display.pop_code,
                    edw_vw_pop6_visits_display.pop_name,
                    edw_vw_pop6_visits_display.address,
                    edw_vw_pop6_visits_display.check_in_longitude,
                    edw_vw_pop6_visits_display.check_in_latitude,
                    edw_vw_pop6_visits_display.check_out_longitude,
                    edw_vw_pop6_visits_display.check_out_latitude,
                    edw_vw_pop6_visits_display.check_in_photo,
                    edw_vw_pop6_visits_display.check_out_photo,
                    edw_vw_pop6_visits_display.username,
                    edw_vw_pop6_visits_display.user_full_name,
                    edw_vw_pop6_visits_display.superior_username,
                    edw_vw_pop6_visits_display.superior_name,
                    edw_vw_pop6_visits_display.planned_visit,
                    edw_vw_pop6_visits_display.cancelled_visit,
                    edw_vw_pop6_visits_display.cancellation_reason,
                    edw_vw_pop6_visits_display.cancellation_note
                FROM edw_vw_pop6_visits_display
                WHERE (
                        (upper((edw_vw_pop6_visits_display.field_type)::TEXT) <> ('PHOTO'::CHARACTER VARYING)::TEXT)
                        AND (upper((edw_vw_pop6_visits_display.field_type)::TEXT) <> ('IMAGE'::CHARACTER VARYING)::TEXT)
                        )
                ) AS rej ON (
                    (
                        (
                            ((vst.visit_id)::TEXT = (rej.visit_id)::TEXT)
                            AND ((vst.product_attribute_value)::TEXT = (rej.product_attribute_value)::TEXT)
                            )
                        AND ((vst.field_id)::TEXT = (rej.dependent_on_field_id)::TEXT)
                        )
                    )
            ) LEFT JOIN (
            -- SELECT DISTINCT edw_vw_ps_weights.kpi,
            --     edw_vw_ps_weights.weight,
            --     CASE 
            --         WHEN ((edw_vw_ps_weights.retail_environment)::TEXT = ('GT Big MM'::CHARACTER VARYING)::TEXT)
            --             THEN 'GT BigMinimart'::CHARACTER VARYING
            --         ELSE edw_vw_ps_weights.retail_environment
            --         END AS retail_environment,
            --     CASE 
            --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('GT'::CHARACTER VARYING)::TEXT)
            --             THEN 'General Trade'::CHARACTER VARYING
            --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('MT'::CHARACTER VARYING)::TEXT)
            --             THEN 'Modern Trade'::CHARACTER VARYING
            --         ELSE edw_vw_ps_weights.channel
            --         END AS channel
            -- FROM edw_vw_ps_weights
            -- WHERE (
            --         (
            --             (upper((edw_vw_ps_weights.kpi)::TEXT) = ('PROMO COMPLIANCE'::CHARACTER VARYING)::TEXT)
            --             OR (upper((edw_vw_ps_weights.kpi)::TEXT) = ('DISPLAY COMPLIANCE'::CHARACTER VARYING)::TEXT)
            --             )
            --         AND (upper((edw_vw_ps_weights.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
            --         )
            SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.weight,
                edw_vw_ps_weights.channel
            FROM edw_vw_ps_weights
            WHERE (
                    upper(edw_vw_ps_weights.kpi::text) = 'PROMO COMPLIANCE'::character varying::text
                    OR upper(edw_vw_ps_weights.kpi::text) = 'DISPLAY COMPLIANCE'::character varying::text
                )
                AND upper(edw_vw_ps_weights.market::text) = 'THAILAND'::character varying::text
            ) AS kpi_wt ON (
                (
                    (
                        (upper((vst.kpi)::TEXT) = upper((kpi_wt.kpi)::TEXT))
                        AND (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                        )
                    AND (upper((kpi_wt.channel)::TEXT) = upper((pop.channel)::TEXT))
                    )
                )
        )
    WHERE (date_part('year', vst.visit_date) >= (date_part('year', '2024-02-09 06:03:00.8736'::TIMESTAMP) - 2))
    ),
ct3
AS (
    SELECT 'Survey_Response' AS dataset,
        NULL AS merchandisingresponseid,
        NULL AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL AS mrch_resp_status,
        NULL AS mastersurveyid,
        NULL AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL AS valuekey,
        NULL AS value,
        NULL AS productid,
        NULL AS mustcarryitem,
        NULL AS presence,
        NULL AS outofstock,
        vst.kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        vst.vst_status,
        NULL AS fisc_yr,
        NULL AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS STATE,
        NULL AS county,
        NULL AS district,
        NULL AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL AS soldtoparty,
        NULL AS productname,
        NULL AS eannumber,
        NULL AS matl_num,
        NULL AS prod_hier_l1,
        NULL AS prod_hier_l2,
        NULL AS prod_hier_l3,
        NULL AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        NULL AS mkt_share,
        NULL AS ques_desc,
        CASE 
            WHEN (upper((rej.response)::TEXT) LIKE ('N.A%'::CHARACTER VARYING)::TEXT)
                THEN ' '::CHARACTER VARYING
            WHEN (
                    (upper((vst.response)::TEXT) = ('YES'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('1'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'YES'::CHARACTER VARYING
            WHEN (
                    (upper((vst.response)::TEXT) = ('NO'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('0'::CHARACTER VARYING)::TEXT)
                    )
                THEN 'NO'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS "y/n_flag",
        CASE 
            WHEN (
                    (upper((vst.response)::TEXT) = ('YES'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('1'::CHARACTER VARYING)::TEXT)
                    )
                THEN ' '::CHARACTER VARYING
            WHEN (
                    (upper((vst.response)::TEXT) = ('NO'::CHARACTER VARYING)::TEXT)
                    OR (upper((vst.response)::TEXT) = ('0'::CHARACTER VARYING)::TEXT)
                    )
                THEN rej.response
            ELSE vst.response
            END AS rej_reason,
        NULL AS response,
        NULL AS response_score,
        NULL AS acc_rej_reason,
        NULL as actual_value,
        NULL as ref_value,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration
    FROM (
        (
            (
                (
                    (
                        (
                            SELECT DISTINCT edw_vw_pop6_th_visits_prod_attribute_audits.cntry_cd,
                                edw_vw_pop6_th_visits_prod_attribute_audits.visit_id,
                                edw_vw_pop6_th_visits_prod_attribute_audits.field_code,
                                edw_vw_pop6_th_visits_prod_attribute_audits.field_label,
                                edw_vw_pop6_th_visits_prod_attribute_audits.field_id,
                                edw_vw_pop6_th_visits_prod_attribute_audits.field_type,
                                edw_vw_pop6_th_visits_prod_attribute_audits.dependent_on_field_id,
                                edw_vw_pop6_th_visits_prod_attribute_audits.response,
                                edw_vw_pop6_th_visits_prod_attribute_audits.visit_date,
                                edw_vw_pop6_th_visits_prod_attribute_audits.check_in_datetime,
                                edw_vw_pop6_th_visits_prod_attribute_audits.check_out_datetime,
                                edw_vw_pop6_th_visits_prod_attribute_audits.popdb_id,
                                edw_vw_pop6_th_visits_prod_attribute_audits.username,
                                edw_vw_pop6_th_visits_prod_attribute_audits.cancelled_visit,
                                edw_vw_pop6_th_visits_prod_attribute_audits.product_attribute,
                                edw_vw_pop6_th_visits_prod_attribute_audits.product_attribute_value,
                                CASE 
                                    WHEN (upper((edw_vw_pop6_th_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('%PS_PROMO%'::CHARACTER VARYING)::TEXT)
                                        THEN 'Promo Compliance'::CHARACTER VARYING
                                    WHEN (upper((edw_vw_pop6_th_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('%PS_DISPLAY%'::CHARACTER VARYING)::TEXT)
                                        THEN 'Display Compliance'::CHARACTER VARYING
                                    WHEN (upper((edw_vw_pop6_th_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('PS_POG_QN_YESNO'::CHARACTER VARYING)::TEXT)
                                        THEN 'Planogram Compliance'::CHARACTER VARYING
                                    ELSE NULL::CHARACTER VARYING
                                    END AS kpi,
                                CASE 
                                    WHEN (edw_vw_pop6_th_visits_prod_attribute_audits.cancelled_visit = 0)
                                        THEN 'completed'::CHARACTER VARYING
                                    ELSE 'cancelled'::CHARACTER VARYING
                                    END AS vst_status
                            FROM edw_vw_pop6_th_visits_prod_attribute_audits
                            WHERE (
                                    (
                                        (upper((edw_vw_pop6_th_visits_prod_attribute_audits.cntry_cd)::TEXT) = ('TH'::CHARACTER VARYING)::TEXT)
                                        AND (
                                            (
                                                (upper((edw_vw_pop6_th_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('%PS_PROMO%'::CHARACTER VARYING)::TEXT)
                                                OR (upper((edw_vw_pop6_th_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('%PS_DISPLAY%'::CHARACTER VARYING)::TEXT)
                                                )
                                            OR (upper((edw_vw_pop6_th_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('%PS_POG%'::CHARACTER VARYING)::TEXT)
                                            )
                                        )
                                    AND (
                                        ((edw_vw_pop6_th_visits_prod_attribute_audits.field_type)::TEXT = ('Yes/No'::CHARACTER VARYING)::TEXT)
                                        OR ((edw_vw_pop6_th_visits_prod_attribute_audits.field_type)::TEXT = ('Yes_No'::CHARACTER VARYING)::TEXT)
                                        )
                                    )
                            ) AS vst LEFT JOIN edw_vw_pop6_store AS pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                        ) LEFT JOIN edw_vw_pop6_salesperson AS usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                    ) LEFT JOIN (
                    SELECT DISTINCT NULL::CHARACTER VARYING AS brand_l4,
                        (upper((edw_vw_pop6_products.ps_category)::TEXT))::CHARACTER VARYING AS ps_category,
                        edw_vw_pop6_products.ps_segment,
                        (((upper((edw_vw_pop6_products.ps_category)::TEXT) || ('_'::CHARACTER VARYING)::TEXT) || (edw_vw_pop6_products.ps_segment)::TEXT))::CHARACTER VARYING AS ps_categorysegement
                    FROM edw_vw_pop6_products
                    
                    UNION
                    
                    SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                        NULL::CHARACTER VARYING AS ps_category,
                        NULL::CHARACTER VARYING AS ps_segment,
                        NULL::CHARACTER VARYING AS ps_categorysegement
                    FROM edw_vw_pop6_products
                    ) AS prd ON (
                        CASE 
                            WHEN ((vst.product_attribute)::TEXT = ('PS Category Segment'::CHARACTER VARYING)::TEXT)
                                THEN ((vst.product_attribute_value)::TEXT = (prd.ps_categorysegement)::TEXT)
                            ELSE ((vst.product_attribute_value)::TEXT = (prd.brand_l4)::TEXT)
                            END
                        )
                ) LEFT JOIN (
                SELECT edw_vw_pop6_visits_prod_attribute_audits.cntry_cd,
                    edw_vw_pop6_visits_prod_attribute_audits.visit_id,
                    edw_vw_pop6_visits_prod_attribute_audits.audit_form_id,
                    edw_vw_pop6_visits_prod_attribute_audits.audit_form,
                    edw_vw_pop6_visits_prod_attribute_audits.section_id,
                    edw_vw_pop6_visits_prod_attribute_audits.section,
                    edw_vw_pop6_visits_prod_attribute_audits.field_id,
                    edw_vw_pop6_visits_prod_attribute_audits.field_code,
                    edw_vw_pop6_visits_prod_attribute_audits.field_label,
                    edw_vw_pop6_visits_prod_attribute_audits.field_type,
                    edw_vw_pop6_visits_prod_attribute_audits.dependent_on_field_id,
                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute_id,
                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute,
                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value_id,
                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value,
                    edw_vw_pop6_visits_prod_attribute_audits.response,
                    edw_vw_pop6_visits_prod_attribute_audits.visit_date,
                    edw_vw_pop6_visits_prod_attribute_audits.check_in_datetime,
                    edw_vw_pop6_visits_prod_attribute_audits.check_out_datetime,
                    edw_vw_pop6_visits_prod_attribute_audits.popdb_id,
                    edw_vw_pop6_visits_prod_attribute_audits.pop_code,
                    edw_vw_pop6_visits_prod_attribute_audits.pop_name,
                    edw_vw_pop6_visits_prod_attribute_audits.address,
                    edw_vw_pop6_visits_prod_attribute_audits.check_in_longitude,
                    edw_vw_pop6_visits_prod_attribute_audits.check_in_latitude,
                    edw_vw_pop6_visits_prod_attribute_audits.check_out_longitude,
                    edw_vw_pop6_visits_prod_attribute_audits.check_out_latitude,
                    edw_vw_pop6_visits_prod_attribute_audits.check_in_photo,
                    edw_vw_pop6_visits_prod_attribute_audits.check_out_photo,
                    edw_vw_pop6_visits_prod_attribute_audits.username,
                    edw_vw_pop6_visits_prod_attribute_audits.user_full_name,
                    edw_vw_pop6_visits_prod_attribute_audits.superior_username,
                    edw_vw_pop6_visits_prod_attribute_audits.superior_name,
                    edw_vw_pop6_visits_prod_attribute_audits.planned_visit,
                    edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit,
                    edw_vw_pop6_visits_prod_attribute_audits.cancellation_reason,
                    edw_vw_pop6_visits_prod_attribute_audits.cancellation_note
                FROM edw_vw_pop6_visits_prod_attribute_audits
                WHERE (
                        (upper((edw_vw_pop6_visits_prod_attribute_audits.field_type)::TEXT) <> ('PHOTO'::CHARACTER VARYING)::TEXT)
                        AND (upper((edw_vw_pop6_visits_prod_attribute_audits.field_type)::TEXT) <> ('IMAGE'::CHARACTER VARYING)::TEXT)
                        )
                ) AS rej ON (
                    (
                        (
                            ((vst.visit_id)::TEXT = (rej.visit_id)::TEXT)
                            AND ((vst.product_attribute_value)::TEXT = (rej.product_attribute_value)::TEXT)
                            )
                        AND ((vst.field_id)::TEXT = (rej.dependent_on_field_id)::TEXT)
                        )
                    )
            ) LEFT JOIN (
            -- SELECT DISTINCT edw_vw_ps_weights.kpi,
            --     edw_vw_ps_weights.weight,
            --     CASE 
            --         WHEN ((edw_vw_ps_weights.retail_environment)::TEXT = ('GT Big MM'::CHARACTER VARYING)::TEXT)
            --             THEN 'GT BigMinimart'::CHARACTER VARYING
            --         ELSE edw_vw_ps_weights.retail_environment
            --         END AS retail_environment,
            --     CASE 
            --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('GT'::CHARACTER VARYING)::TEXT)
            --             THEN 'General Trade'::CHARACTER VARYING
            --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('MT'::CHARACTER VARYING)::TEXT)
            --             THEN 'Modern Trade'::CHARACTER VARYING
            --         ELSE edw_vw_ps_weights.channel
            --         END AS channel
            -- FROM edw_vw_ps_weights
            -- WHERE (
            --         (
            --             (
            --                 (upper((edw_vw_ps_weights.kpi)::TEXT) = ('PROMO COMPLIANCE'::CHARACTER VARYING)::TEXT)
            --                 OR (upper((edw_vw_ps_weights.kpi)::TEXT) = ('DISPLAY COMPLIANCE'::CHARACTER VARYING)::TEXT)
            --                 )
            --             OR (upper((edw_vw_ps_weights.kpi)::TEXT) = ('PLANOGRAM COMPLIANCE'::CHARACTER VARYING)::TEXT)
            --             )
            --         AND (upper((edw_vw_ps_weights.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
            --         )
            SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                edw_vw_ps_weights.kpi,
                edw_vw_ps_weights.weight,
                edw_vw_ps_weights.channel
            FROM edw_vw_ps_weights
            WHERE (
                    upper(edw_vw_ps_weights.kpi::text) = 'PROMO COMPLIANCE'::character varying::text
                    OR upper(edw_vw_ps_weights.kpi::text) = 'DISPLAY COMPLIANCE'::character varying::text
                    OR upper(edw_vw_ps_weights.kpi::text) = 'PLANOGRAM COMPLIANCE'::character varying::text
                )
                AND upper(edw_vw_ps_weights.market::text) = 'THAILAND'::character varying::text
            ) AS kpi_wt ON (
                (
                    (
                        (upper((vst.kpi)::TEXT) = upper((kpi_wt.kpi)::TEXT))
                        AND (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                        )
                    AND (upper((kpi_wt.channel)::TEXT) = upper((pop.channel)::TEXT))
                    )
                )
        )
    WHERE (date_part('year', vst.visit_date) >= (date_part('year', '2024-02-09 06:03:00.8736'::TIMESTAMP) - 2))
    ),
ct4
AS (
    SELECT 'Survey_Response' AS dataset,
        NULL AS merchandisingresponseid,
        NULL AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL AS mrch_resp_status,
        NULL AS mastersurveyid,
        NULL AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL AS valuekey,
        vst.response AS value,
        NULL AS productid,
        NULL AS mustcarryitem,
        NULL AS presence,
        NULL AS outofstock,
        CASE 
            WHEN (upper((vst.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                THEN 'SHARE OF SHELF'::CHARACTER VARYING
            WHEN (upper((vst.kpi)::TEXT) = ('SOA COMPLIANCE'::CHARACTER VARYING)::TEXT)
                THEN 'SHARE OF ASSORTMENT'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        vst.vst_status,
        NULL AS fisc_yr,
        NULL AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS STATE,
        NULL AS county,
        NULL AS district,
        NULL AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL AS soldtoparty,
        NULL AS productname,
        NULL AS eannumber,
        NULL AS matl_num,
        NULL AS prod_hier_l1,
        NULL AS prod_hier_l2,
        NULL AS prod_hier_l3,
        NULL AS prod_hier_l4,
        NULL AS prod_hier_l5,
        NULL AS prod_hier_l6,
        NULL AS prod_hier_l7,
        NULL AS prod_hier_l8,
        NULL AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        CASE 
            WHEN 
            -- ((pop.channel)::TEXT LIKE ('General%'::CHARACTER VARYING)::TEXT)
            pop.channel::text = 'GT%'::character varying::text
                THEN mkt_share_gt."target"
            ELSE mkt_share_mt."target"
            END AS mkt_share,
        CASE 
            WHEN (upper((vst.field_code)::TEXT) = ('PS_SOS_QN_COMP'::CHARACTER VARYING)::TEXT)
                THEN 'denominator'::CHARACTER VARYING
            WHEN (upper((vst.field_code)::TEXT) = ('PS_SOS_QN'::CHARACTER VARYING)::TEXT)
                THEN 'numerator'::CHARACTER VARYING
            ELSE NULL::CHARACTER VARYING
            END AS ques_desc,
        NULL AS "y/n_flag",
        NULL AS rej_reason,
        NULL AS response,
        NULL AS response_score,
        NULL AS acc_rej_reason,
        NULL as actual_value,
        NULL as ref_value,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration
    FROM (
        (
            (
                (
                    (
                        (
                            (
                                SELECT DISTINCT edw_vw_pop6_visits_prod_attribute_audits.cntry_cd,
                                    edw_vw_pop6_visits_prod_attribute_audits.visit_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_code,
                                    CASE 
                                        WHEN (
                                                upper("substring" (
                                                        (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                        1,
                                                        6
                                                        )) = ('PS_SOS'::CHARACTER VARYING)::TEXT
                                                )
                                            THEN 'SOS Compliance'::CHARACTER VARYING
                                        WHEN (
                                                upper("substring" (
                                                        (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                        1,
                                                        6
                                                        )) = ('PS_SOA'::CHARACTER VARYING)::TEXT
                                                )
                                            THEN 'SOA Compliance'::CHARACTER VARYING
                                        ELSE NULL::CHARACTER VARYING
                                        END AS kpi,
                                    CASE 
                                        WHEN (edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit = 0)
                                            THEN 'completed'::CHARACTER VARYING
                                        ELSE 'cancelled'::CHARACTER VARYING
                                        END AS vst_status,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_label,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_type,
                                    edw_vw_pop6_visits_prod_attribute_audits.dependent_on_field_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.response,
                                    edw_vw_pop6_visits_prod_attribute_audits.visit_date,
                                    edw_vw_pop6_visits_prod_attribute_audits.check_in_datetime,
                                    edw_vw_pop6_visits_prod_attribute_audits.check_out_datetime,
                                    edw_vw_pop6_visits_prod_attribute_audits.popdb_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.username,
                                    edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit,
                                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute,
                                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value
                                FROM edw_vw_pop6_visits_prod_attribute_audits
                                WHERE (
                                        (upper((edw_vw_pop6_visits_prod_attribute_audits.cntry_cd)::TEXT) = ('TH'::CHARACTER VARYING)::TEXT)
                                        AND (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) = ('PS_SOS_QN'::CHARACTER VARYING)::TEXT)
                                        )
                                
                                UNION ALL
                                
                                SELECT DISTINCT edw_vw_pop6_visits_prod_attribute_audits.cntry_cd,
                                    edw_vw_pop6_visits_prod_attribute_audits.visit_id,
                                    'PS_SOS_QN_COMP'::CHARACTER VARYING AS field_code,
                                    CASE 
                                        WHEN (
                                                upper("substring" (
                                                        (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                        1,
                                                        6
                                                        )) = ('PS_SOS'::CHARACTER VARYING)::TEXT
                                                )
                                            THEN 'SOS Compliance'::CHARACTER VARYING
                                        WHEN (
                                                upper("substring" (
                                                        (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                        1,
                                                        6
                                                        )) = ('PS_SOA'::CHARACTER VARYING)::TEXT
                                                )
                                            THEN 'SOA Compliance'::CHARACTER VARYING
                                        ELSE NULL::CHARACTER VARYING
                                        END AS kpi,
                                    CASE 
                                        WHEN (edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit = 0)
                                            THEN 'completed'::CHARACTER VARYING
                                        ELSE 'cancelled'::CHARACTER VARYING
                                        END AS vst_status,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_label,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.field_type,
                                    edw_vw_pop6_visits_prod_attribute_audits.dependent_on_field_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.response,
                                    edw_vw_pop6_visits_prod_attribute_audits.visit_date,
                                    edw_vw_pop6_visits_prod_attribute_audits.check_in_datetime,
                                    edw_vw_pop6_visits_prod_attribute_audits.check_out_datetime,
                                    edw_vw_pop6_visits_prod_attribute_audits.popdb_id,
                                    edw_vw_pop6_visits_prod_attribute_audits.username,
                                    edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit,
                                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute,
                                    edw_vw_pop6_visits_prod_attribute_audits.product_attribute_value
                                FROM edw_vw_pop6_visits_prod_attribute_audits
                                WHERE (
                                        (upper((edw_vw_pop6_visits_prod_attribute_audits.cntry_cd)::TEXT) = ('TH'::CHARACTER VARYING)::TEXT)
                                        AND (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE ('PS_SOS_QN%'::CHARACTER VARYING)::TEXT)
                                        )
                                ) AS vst LEFT JOIN edw_vw_pop6_store AS pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                            ) LEFT JOIN edw_vw_pop6_salesperson AS usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                        ) LEFT JOIN (
                        SELECT DISTINCT NULL::CHARACTER VARYING AS brand_l4,
                            (upper((edw_vw_pop6_products.ps_category)::TEXT))::CHARACTER VARYING AS ps_category,
                            edw_vw_pop6_products.ps_segment,
                            (((upper((edw_vw_pop6_products.ps_category)::TEXT) || ('_'::CHARACTER VARYING)::TEXT) || (edw_vw_pop6_products.ps_segment)::TEXT))::CHARACTER VARYING AS ps_categorysegement
                        FROM edw_vw_pop6_products
                        
                        UNION
                        
                        SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                            NULL::CHARACTER VARYING AS ps_category,
                            NULL::CHARACTER VARYING AS ps_segment,
                            NULL::CHARACTER VARYING AS ps_categorysegement
                        FROM edw_vw_pop6_products
                        ) AS prd ON (
                            CASE 
                                WHEN ((vst.product_attribute)::TEXT = ('PS Category Segment'::CHARACTER VARYING)::TEXT)
                                    THEN ((vst.product_attribute_value)::TEXT = (prd.ps_categorysegement)::TEXT)
                                ELSE ((vst.product_attribute_value)::TEXT = (prd.brand_l4)::TEXT)
                                END
                            )
                    ) LEFT JOIN (
                    -- SELECT DISTINCT edw_vw_ps_weights.kpi,
                    --     edw_vw_ps_weights.weight,
                    --     CASE 
                    --         WHEN ((edw_vw_ps_weights.retail_environment)::TEXT = ('GT Big MM'::CHARACTER VARYING)::TEXT)
                    --             THEN 'GT BigMinimart'::CHARACTER VARYING
                    --         ELSE edw_vw_ps_weights.retail_environment
                    --         END AS retail_environment,
                    --     CASE 
                    --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('GT'::CHARACTER VARYING)::TEXT)
                    --             THEN 'General Trade'::CHARACTER VARYING
                    --         WHEN ((edw_vw_ps_weights.channel)::TEXT = ('MT'::CHARACTER VARYING)::TEXT)
                    --             THEN 'Modern Trade'::CHARACTER VARYING
                    --         ELSE edw_vw_ps_weights.channel
                    --         END AS channel
                    -- FROM edw_vw_ps_weights
                    -- WHERE (
                    --         (
                    --             (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                    --             OR (upper((edw_vw_ps_weights.kpi)::TEXT) = ('SOA COMPLIANCE'::CHARACTER VARYING)::TEXT)
                    --             )
                    --         AND (upper((edw_vw_ps_weights.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
                    --         )
                    SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                        edw_vw_ps_weights.kpi,
                        edw_vw_ps_weights.weight,
                        edw_vw_ps_weights.channel
                    FROM edw_vw_ps_weights
                    WHERE (
                            upper(edw_vw_ps_weights.kpi::text) = 'SOS COMPLIANCE'::character varying::text
                            OR upper(edw_vw_ps_weights.kpi::text) = 'SOA COMPLIANCE'::character varying::text
                        )
                        AND upper(edw_vw_ps_weights.market::text) = 'THAILAND'::character varying::text
                    ) AS kpi_wt ON (
                        (
                            (
                                (upper((vst.kpi)::TEXT) = upper((kpi_wt.kpi)::TEXT))
                                AND (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                                )
                            AND (upper((kpi_wt.channel)::TEXT) = upper((pop.channel)::TEXT))
                            )
                        )
                ) LEFT JOIN (
                SELECT DISTINCT edw_vw_ps_targets.kpi,
                    edw_vw_ps_targets.retail_environment,
                    edw_vw_ps_targets.attribute_1 AS segment,
                    edw_vw_ps_targets.value AS "target",
                    edw_vw_ps_targets.channel
                FROM edw_vw_ps_targets
                WHERE (
                        (
                            (
                                (upper((edw_vw_ps_targets.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                OR (upper((edw_vw_ps_targets.kpi)::TEXT) = ('SOA COMPLIANCE'::CHARACTER VARYING)::TEXT)
                                )
                            AND (upper((edw_vw_ps_targets.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
                            )
                        -- AND ((edw_vw_ps_targets.channel)::TEXT = ('General Trade'::CHARACTER VARYING)::TEXT)
                            AND edw_vw_ps_targets.channel::text = 'GT'::character varying::text
                        )
                ) AS mkt_share_gt ON (
                    (
                        (
                            (
                                (upper((vst.kpi)::TEXT) = upper((mkt_share_gt.kpi)::TEXT))
                                AND (upper((prd.ps_segment)::TEXT) = upper((mkt_share_gt.segment)::TEXT))
                                )
                            AND (upper((mkt_share_gt.channel)::TEXT) = upper((pop.channel)::TEXT))
                            )
                        AND (upper((pop.retail_environment_ps)::TEXT) = upper((mkt_share_gt.retail_environment)::TEXT))
                        )
                    )
            ) LEFT JOIN (
            SELECT DISTINCT edw_vw_ps_targets.kpi,
                edw_vw_ps_targets.retail_environment,
                edw_vw_ps_targets.attribute_1 AS segment,
                edw_vw_ps_targets.attribute_2 AS customer,
                edw_vw_ps_targets.value AS "target",
                edw_vw_ps_targets.channel
            FROM edw_vw_ps_targets
            WHERE (
                    (
                        (
                            (upper((edw_vw_ps_targets.kpi)::TEXT) = ('SOS COMPLIANCE'::CHARACTER VARYING)::TEXT)
                            OR (upper((edw_vw_ps_targets.kpi)::TEXT) = ('SOA COMPLIANCE'::CHARACTER VARYING)::TEXT)
                            )
                        AND (upper((edw_vw_ps_targets.market)::TEXT) = ('THAILAND'::CHARACTER VARYING)::TEXT)
                        )
                    -- AND ((edw_vw_ps_targets.channel)::TEXT = ('Modern Trade'::CHARACTER VARYING)::TEXT)
                    AND edw_vw_ps_targets.channel::text = 'MT'::character varying::text
                    )
            ) AS mkt_share_mt ON (
                (
                    (
                        (
                            (
                                (upper((vst.kpi)::TEXT) = upper((mkt_share_mt.kpi)::TEXT))
                                AND (upper((prd.ps_segment)::TEXT) = upper((mkt_share_mt.segment)::TEXT))
                                )
                            AND (upper((mkt_share_mt.channel)::TEXT) = upper((pop.channel)::TEXT))
                            )
                        AND (upper((pop.retail_environment_ps)::TEXT) = upper((mkt_share_mt.retail_environment)::TEXT))
                        )
                    AND (upper((pop.customer)::TEXT) = upper((mkt_share_mt.customer)::TEXT))
                    )
                )
        )
    WHERE (date_part('year', vst.visit_date) >= (date_part('year', '2024-02-09 06:03:00.8736'::TIMESTAMP) - 2))
    ),
ct5 
AS (
        select 'IR_Response' as dataset,
            null as merchandisingresponseid,
            null as surveyresponseid,
            main.popdb_id  as customerid,
            usr.userdb_id as salespersonid,
            main.visit_id as visitid,
            main.check_in_datetime as mrch_resp_startdt,
            main.check_out_datetime as mrch_resp_enddt,
            null as mrch_resp_status,
            null as mastersurveyid,
            null as survey_status,
            main.check_out_datetime as survey_enddate,
            questionkey,
            questiontext,
            null as valuekey,
            null as value,
            null as productid,
            null as mustcarryitem,
            null as presence,
            null as outofstock, 
            main.kpi,
            main.ps_category as category,
            main.ps_segment as segment,
            main.visit_id as vst_visitid,
            main.visit_date as scheduleddate,
            main.check_in_datetime as scheduledtime,
            main.vst_status,
            NULL AS fisc_yr,
            NULL AS fisc_per,
            usr.first_name AS firstname,
            usr.last_name AS lastname,
            main.popdb_id as  cust_remotekey,
            pop.pop_name AS customername,
            pop.country,
            pop.territory_or_region AS STATE,
            NULL AS county,
            NULL AS district,
            NULL AS city,
            pop.customer AS storereference,
            pop.retail_environment_ps AS storetype,
            pop.channel,
            pop.sales_group_name AS salesgroup,
            pop.business_unit_name AS bu,
            NULL AS soldtoparty,
            NULL AS productname,
            NULL AS eannumber,
            NULL AS matl_num,
            NULL AS prod_hier_l1,
            NULL AS prod_hier_l2,
            NULL AS prod_hier_l3,
            NULL AS prod_hier_l4,
            NULL AS prod_hier_l5,
            NULL AS prod_hier_l6,
            NULL AS prod_hier_l7,
            NULL AS prod_hier_l8,
            NULL AS prod_hier_l9,
            kpi_wt.weight as kpi_chnl_wt,
            null as mkt_share,
            null as ques_desc,
            null as "y/n_flag",
            null as rej_reason,
            null as response,
            null as response_score,
            null as acc_rej_reason,
            case when main.ques_desc = 'numerator' then value::varchar(20) else '0' end as actual_value,
            case when main.ques_desc = 'denominator' then value::varchar(20) else '0' end  as ref_value,
            datediff(SECOND, to_date(main.check_in_datetime::VARCHAR), to_date(main.check_out_datetime::VARCHAR)) AS duration    
            from
    (select cntry_cd,
            visit_id,
            field_code as questionkey,
            kpi,
            vst_status,
            questiontext,
            ques_desc,
            sum(facing_of_this_layer) as value,
            visit_date,
            check_in_datetime,
            check_out_datetime,
            popdb_id,
            pop_code,
            pop_name,
            username,
            ps_category,
            ps_segment
    from  (select  cntry_cd ,
                    visit_id,
                    'PS_POG_QN' as field_code,
                    'Planogram Compliance' as kpi,
                    case  when cancelled_visit = 0 then 'completed'else 'cancelled' end as vst_status,
                    'How many Hand-Eye facing Kenvue acquired?' as questiontext,
                    'numerator' as ques_desc,
                    facing_of_this_layer,
                    layer,
                    total_layer,
                    visit_date,
                    check_in_datetime,
                    check_out_datetime,
                    popdb_id,
                    pop_code,
                    pop_name,
                    username,
                    ps_category,
                    ps_segment
        from edw_vw_pop6_visits_rir_data
        where upper(company) in ('KENVUE','JNJ','KV')
    UNION ALL
    select  cntry_cd ,
                    visit_id,
                    'PS_POG_QN_COMP' as field_code,
                    'Planogram Compliance' as kpi,
                    case  when cancelled_visit = 0 then 'completed'else 'cancelled' end as vst_status,
                    case when upper(company) in ('KENVUE','JNJ','KV') then 'How many Hand-Eye facing Kenvue acquired?'
                    else 'How many Hand-Eye facing Competitor acquired?' end  as questiontext,
                    'denominator' as ques_desc,
                    facing_of_this_layer,
                    layer,
                    total_layer,
                    visit_date,
                    check_in_datetime,
                    check_out_datetime,
                    popdb_id,
                    pop_code,
                    pop_name,
                    username,
                    ps_category,
                    ps_segment
        from edw_vw_pop6_visits_rir_data) fact
        inner join itg_mds_th_ps_handeye_level he_lvl
        on he_lvl.total_layers = fact.total_layer and he_lvl.hand_eye_layer = fact.layer
    where   (date_part('year', visit_date) >= (date_part('year',  sysdate()) - 2))
        group by cntry_cd,
                visit_id,
                field_code,
                kpi,
                vst_status,
                questiontext,
                ques_desc,
                visit_date,
                check_in_datetime,
                check_out_datetime,
                popdb_id,
                pop_code,
                pop_name,
                username,
                ps_category,
                ps_segment) main
    left join edw_vw_pop6_salesperson usr
    on upper(usr.username) = upper(main.username)
    left join edw_vw_pop6_store pop
    on pop.popdb_id  = main.popdb_id
    left join edw_vw_ps_weights kpi_wt 
    on upper(kpi_wt.channel) = upper(pop.channel) and upper(pop.retail_environment_ps) = upper(kpi_wt.retail_environment)
    and upper(kpi_wt.market) = 'THAILAND' and upper(kpi_wt.kpi) = 'PLANOGRAM COMPLIANCE'

    ),
ct6  
AS (  
        select 'IR_Response' as dataset,
                null as merchandisingresponseid,
                null as surveyresponseid,
                main.popdb_id  as customerid,
                usr.userdb_id as salespersonid,
                main.visit_id as visitid,
                main.check_in_datetime as mrch_resp_startdt,
                main.check_out_datetime as mrch_resp_enddt,
                null as mrch_resp_status,
                null as mastersurveyid,
                null as survey_status,
                main.check_out_datetime as survey_enddate,
                questionkey,
                questiontext,
                null as valuekey,
                main.value::varchar(100) as value ,
                null as productid,
                null as mustcarryitem,
                null as presence,
                null as outofstock, 
                main.kpi,
                main.ps_category as category,
                main.ps_segment as segment,
                main.visit_id as vst_visitid,
                main.visit_date as scheduleddate,
                main.check_in_datetime as scheduledtime,
                main.vst_status,
                NULL AS fisc_yr,
                NULL AS fisc_per,
                usr.first_name AS firstname,
                usr.last_name AS lastname,
                main.popdb_id as  cust_remotekey,
                pop.pop_name AS customername,
                pop.country,
                pop.territory_or_region AS STATE,
                NULL AS county,
                NULL AS district,
                NULL AS city,
                pop.customer AS storereference,
                pop.retail_environment_ps AS storetype,
                pop.channel,
                pop.sales_group_name AS salesgroup,
                pop.business_unit_name AS bu,
                NULL AS soldtoparty,
                NULL AS productname,
                NULL AS eannumber,
                NULL AS matl_num,
                NULL AS prod_hier_l1,
                NULL AS prod_hier_l2,
                NULL AS prod_hier_l3,
                NULL AS prod_hier_l4,
                NULL AS prod_hier_l5,
                NULL AS prod_hier_l6,
                NULL AS prod_hier_l7,
                NULL AS prod_hier_l8,
                NULL AS prod_hier_l9,
                kpi_wt.weight as kpi_chnl_wt,
                case when pop.channel = 'GT' then kpi_tgt_gt.value else kpi_tgt_mt.value end  as mkt_share,
                main.ques_desc,
                null as "y/n_flag",
                null as rej_reason,
                null as response,
                null as response_score,
                null as acc_rej_reason,
                NULL as actual_value,
                NULL as ref_value,
                datediff(SECOND, to_date(main.check_in_datetime::VARCHAR), to_date(main.check_out_datetime::VARCHAR)) AS duration     
        from 
        (select cntry_cd,
                visit_id,
                field_code as questionkey,
                kpi,
                vst_status,
                questiontext,
                ques_desc,
                sum(facing_of_this_layer)::numeric(20,2) as value,
                visit_date,
                check_in_datetime,
                check_out_datetime,
                popdb_id,
                pop_code,
                pop_name,
                username,
                ps_category,
                ps_segment
        from  (select  cntry_cd ,
                        visit_id,
                        'PS_SOS_QN' as field_code,
                        'SHARE OF SHELF' as kpi,
                        case  when cancelled_visit = 0 then 'completed'else 'cancelled' end as vst_status,
                        'How many facing Kenvue acquired?' as questiontext,
                        'numerator' as ques_desc,
                        facing_of_this_layer,
                        visit_date,
                        check_in_datetime,
                        check_out_datetime,
                        popdb_id,
                        pop_code,
                        pop_name,
                        username,
                        ps_category,
                        ps_segment
            from edw_vw_pop6_visits_rir_data
            where upper(company) in ('KENVUE','JNJ','KV')
        UNION ALL
        select  cntry_cd ,
                        visit_id,
                        'PS_SOS_QN_COMP' as field_code,
                        'SHARE OF SHELF' as kpi,
                        case  when cancelled_visit = 0 then 'completed'else 'cancelled' end as vst_status,
                        case when upper(company) in ('KENVUE','JNJ','KV') then 'How many facing Kenvue acquired?'
                        else 'How many facing Competitor acquired?' end  as questiontext,
                        'denominator' as ques_desc,
                        facing_of_this_layer,
                        visit_date,
                        check_in_datetime,
                        check_out_datetime,
                        popdb_id,
                        pop_code,
                        pop_name,
                        username,
                        ps_category,
                        ps_segment
            from edw_vw_pop6_visits_rir_data)
        where   (date_part('year', visit_date) >= (date_part('year',  sysdate()) - 2))
            group by cntry_cd,
                    visit_id,
                    field_code,
                    kpi,
                    vst_status,
                    questiontext,
                    ques_desc,
                    visit_date,
                    check_in_datetime,
                    check_out_datetime,
                    popdb_id,
                    pop_code,
                    pop_name,
                    username,
                    ps_category,
                    ps_segment) main
        left join edw_vw_pop6_salesperson usr
        on upper(usr.username) = upper(main.username)
        left join edw_vw_pop6_store pop
        on pop.popdb_id  = main.popdb_id
        left join edw_vw_ps_weights kpi_wt 
        on upper(kpi_wt.channel) = upper(pop.channel) and upper(pop.retail_environment_ps) = upper(kpi_wt.retail_environment)
        and upper(kpi_wt.market) = 'THAILAND' and upper(kpi_wt.kpi) = 'SOS COMPLIANCE'
        left join edw_vw_ps_targets kpi_tgt_gt
        on upper(kpi_tgt_gt.channel) =  upper(pop.channel) and upper(pop.retail_environment_ps) = upper(kpi_tgt_gt.retail_environment)
        and upper(main.ps_segment) = upper(kpi_tgt_gt.attribute_1) and upper(kpi_tgt_gt.market) = 'THAILAND' and upper(kpi_tgt_gt.kpi) = 'SOS COMPLIANCE'
        and upper(kpi_tgt_gt.channel) = 'GT'
        left join edw_vw_ps_targets kpi_tgt_mt 
        on upper(kpi_tgt_mt.channel) =  upper(pop.channel) and upper(pop.retail_environment_ps) = upper(kpi_tgt_mt.retail_environment)
        and upper(main.ps_segment) = upper(kpi_tgt_mt.attribute_1) and upper(pop.customer) = upper(kpi_tgt_mt.attribute_2)
        and upper(kpi_tgt_mt.market) = 'THAILAND' and upper(kpi_tgt_mt.kpi) = 'SOS COMPLIANCE'
        and upper(kpi_tgt_mt.channel) = 'MT'
    ),
final
AS (
    SELECT *
    FROM ct1
    
    UNION ALL
    
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