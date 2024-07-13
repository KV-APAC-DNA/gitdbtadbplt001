WITH edw_vw_pop6_visits_sku_audits
AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_pop6_visits_sku_audits') }}
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
edw_vw_ps_weights AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_ps_weights') }}
),

edw_vw_ps_targets AS (
    SELECT *
    FROM {{ ref('aspedw_integration__edw_vw_ps_targets') }}
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
        (
            CASE 
                WHEN (
                        (upper((vst.response)::TEXT) = 'YES'::TEXT)
                        OR (upper((vst.response)::TEXT) = (1)::TEXT)
                        )
                    THEN 'true'::TEXT
                ELSE 'false'::TEXT
                END
            )::CHARACTER VARYING AS presence,
        NULL AS outofstock,
        'MSL Compliance' AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration,
        (
            CASE 
                WHEN (vst.cancelled_visit = 0)
                    THEN 'completed'::TEXT
                ELSE 'cancelled'::TEXT
                END
            )::CHARACTER VARYING AS vst_status,
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
        NULL AS acc_rej_reason
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
                        WHERE (upper((edw_vw_pop6_visits_sku_audits.cntry_cd)::TEXT) = 'JP'::TEXT)
                        ) vst LEFT JOIN edw_vw_pop6_store pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                    ) LEFT JOIN (
                    SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                        edw_vw_ps_weights.kpi,
                        (edw_vw_ps_weights.weight)::NUMERIC(20, 4) AS weight
                    FROM edw_vw_ps_weights
                    WHERE (
                            (
                                (upper((edw_vw_ps_weights.kpi)::TEXT) = 'MSL COMPLIANCE'::TEXT)
                                AND (upper((edw_vw_ps_weights.channel)::TEXT) = 'MODERN TRADE'::TEXT)
                                )
                            AND (upper((edw_vw_ps_weights.market)::TEXT) = 'JAPAN'::TEXT)
                            )
                    ) kpi_wt ON ((upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT)))
                ) LEFT JOIN edw_vw_pop6_salesperson usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
            ) LEFT JOIN edw_vw_pop6_products prd ON (((vst.sku_id)::TEXT = (prd.productdb_id)::TEXT))
        )
    WHERE (
            (upper((vst.field_code)::TEXT) = 'PS_MSL'::TEXT)
            AND (date_part(year, (vst.visit_date)::TIMESTAMP without TIME zone) >= (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - (2)::DOUBLE PRECISION))
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
        (
            CASE 
                WHEN (
                        (upper((vst.response)::TEXT) = 'NO'::TEXT)
                        OR (upper((vst.response)::TEXT) = (0)::TEXT)
                        )
                    THEN COALESCE(CASE 
                                WHEN (trim((vst_oos.response)::TEXT) = ''::TEXT)
                                    THEN NULL::TEXT
                                ELSE trim((vst_oos.response)::TEXT)
                                END, 'other'::TEXT)
                ELSE ''::TEXT
                END
            )::CHARACTER VARYING AS outofstock,
        'OOS Compliance' AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration,
        (
            CASE 
                WHEN (vst.cancelled_visit = 0)
                    THEN 'completed'::TEXT
                ELSE 'cancelled'::TEXT
                END
            )::CHARACTER VARYING AS vst_status,
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
        NULL AS acc_rej_reason
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
                            WHERE (upper((edw_vw_pop6_visits_sku_audits.cntry_cd)::TEXT) = 'JP'::TEXT)
                            ) vst LEFT JOIN edw_vw_pop6_store pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                        ) LEFT JOIN (
                        SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                            edw_vw_ps_weights.kpi,
                            (edw_vw_ps_weights.weight)::NUMERIC(20, 4) AS weight
                        FROM edw_vw_ps_weights
                        WHERE (
                                (
                                    (upper((edw_vw_ps_weights.kpi)::TEXT) = 'OSA COMPLIANCE'::TEXT)
                                    AND (upper((edw_vw_ps_weights.channel)::TEXT) = 'MODERN TRADE'::TEXT)
                                    )
                                AND (upper((edw_vw_ps_weights.market)::TEXT) = 'JAPAN'::TEXT)
                                )
                        ) kpi_wt ON ((upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT)))
                    ) LEFT JOIN edw_vw_pop6_salesperson usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                ) LEFT JOIN edw_vw_pop6_products prd ON (((vst.sku_id)::TEXT = (prd.productdb_id)::TEXT))
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
            WHERE (upper((edw_vw_pop6_visits_sku_audits.field_code)::TEXT) = 'PS_MSL_OOS_REASON'::TEXT)
            ) vst_oos ON (
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
            (upper((vst.field_code)::TEXT) = 'PS_MSL_OOS'::TEXT)
            AND (date_part(year, (vst.visit_date)::TIMESTAMP without TIME zone) >= (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - (2)::DOUBLE PRECISION))
            )),
        ct2 AS (
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
                (vst.kpi)::CHARACTER VARYING AS kpi,
                prd.ps_category AS category,
                prd.ps_segment AS segment,
                vst.visit_id AS vst_visitid,
                vst.visit_date AS scheduleddate,
                vst.check_in_datetime AS scheduledtime,
                datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration,
                (vst.vst_status)::CHARACTER VARYING AS vst_status,
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
                (
                    CASE 
                        WHEN (upper((rej.response)::TEXT) LIKE 'N.A%'::TEXT)
                            THEN ' '::TEXT
                        WHEN (
                                (upper((vst.response)::TEXT) = 'YES'::TEXT)
                                OR (upper((vst.response)::TEXT) = '1'::TEXT)
                                )
                            THEN 'YES'::TEXT
                        WHEN (
                                (upper((vst.response)::TEXT) = 'NO'::TEXT)
                                OR (upper((vst.response)::TEXT) = '0'::TEXT)
                                )
                            THEN 'NO'::TEXT
                        ELSE NULL::TEXT
                        END
                    )::CHARACTER VARYING AS "y/n_flag",
                CASE 
                    WHEN (
                            (upper((vst.response)::TEXT) = 'YES'::TEXT)
                            OR (upper((vst.response)::TEXT) = '1'::TEXT)
                            )
                        THEN ' '::CHARACTER VARYING
                    WHEN (
                            (upper((vst.response)::TEXT) = 'NO'::TEXT)
                            OR (upper((vst.response)::TEXT) = '0'::TEXT)
                            )
                        THEN rej.response
                    ELSE vst.response
                    END AS rej_reason,
                NULL AS response,
                NULL AS response_score,
                NULL AS acc_rej_reason
            FROM (
                (
                    (
                        (
                            (
                                (
                                    SELECT DISTINCT edw_vw_pop6_visits_display.cntry_cd,
                                        edw_vw_pop6_visits_display.visit_id,
                                        edw_vw_pop6_visits_display.field_code,
                                        CASE 
                                            WHEN (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE '%PS_PROMO%'::TEXT)
                                                THEN 'Promo Compliance'::TEXT
                                            WHEN (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE '%PS_DISPLAY%'::TEXT)
                                                THEN 'Display Compliance'::TEXT
                                            ELSE NULL::TEXT
                                            END AS kpi,
                                        CASE 
                                            WHEN (edw_vw_pop6_visits_display.cancelled_visit = 0)
                                                THEN 'completed'::TEXT
                                            ELSE 'cancelled'::TEXT
                                            END AS vst_status,
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
                                        edw_vw_pop6_visits_display.product_attribute_value
                                    FROM edw_vw_pop6_visits_display
                                    WHERE (
                                            (
                                                (upper((edw_vw_pop6_visits_display.cntry_cd)::TEXT) = 'JP'::TEXT)
                                                AND (
                                                    (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE '%PS_PROMO%'::TEXT)
                                                    OR (upper((edw_vw_pop6_visits_display.field_code)::TEXT) LIKE '%PS_DISPLAY%'::TEXT)
                                                    )
                                                )
                                            AND (
                                                ((edw_vw_pop6_visits_display.field_type)::TEXT = 'Yes/No'::TEXT)
                                                OR ((edw_vw_pop6_visits_display.field_type)::TEXT = 'Yes_No'::TEXT)
                                                )
                                            )
                                    ) vst LEFT JOIN edw_vw_pop6_store pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                                ) LEFT JOIN edw_vw_pop6_salesperson usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                            ) LEFT JOIN (
                            SELECT DISTINCT NULL::TEXT AS brand_l4,
                                (upper((edw_vw_pop6_products.ps_category)::TEXT))::CHARACTER VARYING AS ps_category,
                                edw_vw_pop6_products.ps_segment,
                                (((upper((edw_vw_pop6_products.ps_category)::TEXT) || '_'::TEXT) || (edw_vw_pop6_products.ps_segment)::TEXT))::CHARACTER VARYING AS ps_categorysegement
                            FROM edw_vw_pop6_products
                            
                            UNION
                            
                            SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                                NULL::TEXT AS ps_category,
                                NULL::TEXT AS ps_segment,
                                NULL::TEXT AS ps_categorysegement
                            FROM edw_vw_pop6_products
                            ) prd ON (
                                CASE 
                                    WHEN ((vst.product_attribute)::TEXT = 'PS Category Segment'::TEXT)
                                        THEN ((vst.product_attribute_value)::TEXT = (prd.ps_categorysegement)::TEXT)
                                    ELSE ((vst.product_attribute_value)::TEXT = prd.brand_l4)
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
                        WHERE (upper((edw_vw_pop6_visits_display.field_type)::TEXT) <> 'PHOTO'::TEXT)
                        ) rej ON (
                            (
                                (
                                    ((vst.visit_id)::TEXT = (rej.visit_id)::TEXT)
                                    AND ((vst.product_attribute_value)::TEXT = (rej.product_attribute_value)::TEXT)
                                    )
                                AND ((vst.field_id)::TEXT = (rej.dependent_on_field_id)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                        edw_vw_ps_weights.kpi,
                        (edw_vw_ps_weights.weight)::NUMERIC(20, 4) AS weight
                    FROM edw_vw_ps_weights
                    WHERE (
                            (
                                (
                                    (upper((edw_vw_ps_weights.kpi)::TEXT) = 'PROMO COMPLIANCE'::TEXT)
                                    OR (upper((edw_vw_ps_weights.kpi)::TEXT) = 'DISPLAY COMPLIANCE'::TEXT)
                                    )
                                AND (upper((edw_vw_ps_weights.channel)::TEXT) = 'MODERN TRADE'::TEXT)
                                )
                            AND (upper((edw_vw_ps_weights.market)::TEXT) = 'JAPAN'::TEXT)
                            )
                    ) kpi_wt ON (
                        (
                            (upper(vst.kpi) = upper((kpi_wt.kpi)::TEXT))
                            AND (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                            )
                        )
                )
            WHERE (date_part(year, (vst.visit_date)::TIMESTAMP without TIME zone) >= (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - (2)::DOUBLE PRECISION))
            ),
        ct3 AS (
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
                (vst.kpi)::CHARACTER VARYING AS kpi,
                prd.ps_category AS category,
                prd.ps_segment AS segment,
                vst.visit_id AS vst_visitid,
                vst.visit_date AS scheduleddate,
                vst.check_in_datetime AS scheduledtime,
                datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration,
                (vst.vst_status)::CHARACTER VARYING AS vst_status,
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
                (
                    CASE 
                        WHEN (upper((rej.response)::TEXT) LIKE 'N.A%'::TEXT)
                            THEN ' '::TEXT
                        WHEN (
                                (upper((vst.response)::TEXT) = 'YES'::TEXT)
                                OR (upper((vst.response)::TEXT) = '1'::TEXT)
                                )
                            THEN 'YES'::TEXT
                        WHEN (
                                (upper((vst.response)::TEXT) = 'NO'::TEXT)
                                OR (upper((vst.response)::TEXT) = '0'::TEXT)
                                )
                            THEN 'NO'::TEXT
                        ELSE NULL::TEXT
                        END
                    )::CHARACTER VARYING AS "y/n_flag",
                CASE 
                    WHEN (
                            (upper((vst.response)::TEXT) = 'YES'::TEXT)
                            OR (upper((vst.response)::TEXT) = '1'::TEXT)
                            )
                        THEN ' '::CHARACTER VARYING
                    WHEN (
                            (upper((vst.response)::TEXT) = 'NO'::TEXT)
                            OR (upper((vst.response)::TEXT) = '0'::TEXT)
                            )
                        THEN rej.response
                    ELSE vst.response
                    END AS rej_reason,
                NULL AS response,
                NULL AS response_score,
                NULL AS acc_rej_reason
            FROM (
                (
                    (
                        (
                            (
                                (
                                    SELECT DISTINCT edw_vw_pop6_visits_prod_attribute_audits.cntry_cd,
                                        edw_vw_pop6_visits_prod_attribute_audits.visit_id,
                                        edw_vw_pop6_visits_prod_attribute_audits.field_code,
                                        CASE 
                                            WHEN (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE '%PS_PROMO%'::TEXT)
                                                THEN 'Promo Compliance'::TEXT
                                            WHEN (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE '%PS_DISPLAY%'::TEXT)
                                                THEN 'Display Compliance'::TEXT
                                            WHEN (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE '%PS_POG%'::TEXT)
                                                THEN 'Planogram Compliance'::TEXT
                                            ELSE NULL::TEXT
                                            END AS kpi,
                                        CASE 
                                            WHEN (edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit = 0)
                                                THEN 'completed'::TEXT
                                            ELSE 'cancelled'::TEXT
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
                                            (
                                                (upper((edw_vw_pop6_visits_prod_attribute_audits.cntry_cd)::TEXT) = 'JP'::TEXT)
                                                AND (
                                                    (
                                                        (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE '%PS_PROMO%'::TEXT)
                                                        OR (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE '%PS_DISPLAY%'::TEXT)
                                                        )
                                                    OR (upper((edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT) LIKE '%PS_POG%'::TEXT)
                                                    )
                                                )
                                            AND (
                                                ((edw_vw_pop6_visits_prod_attribute_audits.field_type)::TEXT = 'Yes/No'::TEXT)
                                                OR ((edw_vw_pop6_visits_prod_attribute_audits.field_type)::TEXT = 'Yes_No'::TEXT)
                                                )
                                            )
                                    ) vst LEFT JOIN edw_vw_pop6_store pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                                ) LEFT JOIN edw_vw_pop6_salesperson usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                            ) LEFT JOIN (
                            SELECT DISTINCT NULL::TEXT AS brand_l4,
                                (upper((edw_vw_pop6_products.ps_category)::TEXT))::CHARACTER VARYING AS ps_category,
                                edw_vw_pop6_products.ps_segment,
                                (((upper((edw_vw_pop6_products.ps_category)::TEXT) || '_'::TEXT) || (edw_vw_pop6_products.ps_segment)::TEXT))::CHARACTER VARYING AS ps_categorysegement
                            FROM edw_vw_pop6_products
                            
                            UNION
                            
                            SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                                NULL::TEXT AS ps_category,
                                NULL::TEXT AS ps_segment,
                                NULL::TEXT AS ps_categorysegement
                            FROM edw_vw_pop6_products
                            ) prd ON (
                                CASE 
                                    WHEN ((vst.product_attribute)::TEXT = 'PS Category Segment'::TEXT)
                                        THEN ((vst.product_attribute_value)::TEXT = (prd.ps_categorysegement)::TEXT)
                                    ELSE ((vst.product_attribute_value)::TEXT = prd.brand_l4)
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
                        WHERE (upper((edw_vw_pop6_visits_prod_attribute_audits.field_type)::TEXT) <> 'PHOTO'::TEXT)
                        ) rej ON (
                            (
                                (
                                    ((vst.visit_id)::TEXT = (rej.visit_id)::TEXT)
                                    AND ((vst.product_attribute_value)::TEXT = (rej.product_attribute_value)::TEXT)
                                    )
                                AND ((vst.field_id)::TEXT = (rej.dependent_on_field_id)::TEXT)
                                )
                            )
                    ) LEFT JOIN (
                    SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                        edw_vw_ps_weights.kpi,
                        (edw_vw_ps_weights.weight)::NUMERIC(20, 4) AS weight
                    FROM edw_vw_ps_weights
                    WHERE (
                            (
                                (
                                    (
                                        (upper((edw_vw_ps_weights.kpi)::TEXT) = 'PROMO COMPLIANCE'::TEXT)
                                        OR (upper((edw_vw_ps_weights.kpi)::TEXT) = 'DISPLAY COMPLIANCE'::TEXT)
                                        )
                                    OR (upper((edw_vw_ps_weights.kpi)::TEXT) = 'PLANOGRAM COMPLIANCE'::TEXT)
                                    )
                                AND (upper((edw_vw_ps_weights.channel)::TEXT) = 'MODERN TRADE'::TEXT)
                                )
                            AND (upper((edw_vw_ps_weights.market)::TEXT) = 'JAPAN'::TEXT)
                            )
                    ) kpi_wt ON (
                        (
                            (upper(vst.kpi) = upper((kpi_wt.kpi)::TEXT))
                            AND (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                            )
                        )
                )
            WHERE (date_part(year, (vst.visit_date)::TIMESTAMP without TIME zone) >= (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - (2)::DOUBLE PRECISION))
            ),
        ct4 AS (
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
                (
                    CASE 
                        WHEN (upper(vst.kpi) = 'SOS COMPLIANCE'::TEXT)
                            THEN 'SHARE OF SHELF'::TEXT
                        WHEN (upper(vst.kpi) = 'SOA COMPLIANCE'::TEXT)
                            THEN 'SHARE OF ASSORTMENT'::TEXT
                        ELSE NULL::TEXT
                        END
                    )::CHARACTER VARYING AS kpi,
                prd.ps_category AS category,
                prd.ps_segment AS segment,
                vst.visit_id AS vst_visitid,
                vst.visit_date AS scheduleddate,
                vst.check_in_datetime AS scheduledtime,
                datediff(SECOND, to_date(vst.check_in_datetime::VARCHAR), to_date(vst.check_out_datetime::VARCHAR)) AS duration,
                (vst.vst_status)::CHARACTER VARYING AS vst_status,
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
                mkt_share."target" AS mkt_share,
                (
                    CASE 
                        WHEN (upper((vst.field_code)::TEXT) LIKE '%Q1%'::TEXT)
                            THEN 'denominator'::TEXT
                        WHEN (upper((vst.field_code)::TEXT) LIKE '%QN%'::TEXT)
                            THEN 'numerator'::TEXT
                        ELSE NULL::TEXT
                        END
                    )::CHARACTER VARYING AS ques_desc,
                NULL AS "y/n_flag",
                NULL AS rej_reason,
                NULL AS response,
                NULL AS response_score,
                NULL AS acc_rej_reason
            FROM (
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
                                                            )) = 'PS_SOS'::TEXT
                                                    )
                                                THEN 'SOS Compliance'::TEXT
                                            WHEN (
                                                    upper("substring" (
                                                            (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                            1,
                                                            6
                                                            )) = 'PS_SOA'::TEXT
                                                    )
                                                THEN 'SOA Compliance'::TEXT
                                            ELSE NULL::TEXT
                                            END AS kpi,
                                        CASE 
                                            WHEN (edw_vw_pop6_visits_prod_attribute_audits.cancelled_visit = 0)
                                                THEN 'completed'::TEXT
                                            ELSE 'cancelled'::TEXT
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
                                            (upper((edw_vw_pop6_visits_prod_attribute_audits.cntry_cd)::TEXT) = 'JP'::TEXT)
                                            AND (
                                                (
                                                    upper("substring" (
                                                            (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                            1,
                                                            6
                                                            )) = 'PS_SOS'::TEXT
                                                    )
                                                OR (
                                                    upper("substring" (
                                                            (edw_vw_pop6_visits_prod_attribute_audits.field_code)::TEXT,
                                                            1,
                                                            6
                                                            )) = 'PS_SOA'::TEXT
                                                    )
                                                )
                                            )
                                    ) vst LEFT JOIN edw_vw_pop6_store pop ON (((vst.popdb_id)::TEXT = (pop.popdb_id)::TEXT))
                                ) LEFT JOIN edw_vw_pop6_salesperson usr ON (((vst.username)::TEXT = (usr.username)::TEXT))
                            ) LEFT JOIN (
                            SELECT DISTINCT NULL::TEXT AS brand_l4,
                                (upper((edw_vw_pop6_products.ps_category)::TEXT))::CHARACTER VARYING AS ps_category,
                                edw_vw_pop6_products.ps_segment,
                                (((upper((edw_vw_pop6_products.ps_category)::TEXT) || '_'::TEXT) || (edw_vw_pop6_products.ps_segment)::TEXT))::CHARACTER VARYING AS ps_categorysegement
                            FROM edw_vw_pop6_products
                            
                            UNION
                            
                            SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                                NULL::TEXT AS ps_category,
                                NULL::TEXT AS ps_segment,
                                NULL::TEXT AS ps_categorysegement
                            FROM edw_vw_pop6_products
                            ) prd ON (
                                CASE 
                                    WHEN ((vst.product_attribute)::TEXT = 'PS Category Segment'::TEXT)
                                        THEN ((vst.product_attribute_value)::TEXT = (prd.ps_categorysegement)::TEXT)
                                    ELSE ((vst.product_attribute_value)::TEXT = prd.brand_l4)
                                    END
                                )
                        ) LEFT JOIN (
                        SELECT DISTINCT edw_vw_ps_weights.retail_environment,
                            edw_vw_ps_weights.kpi,
                            (edw_vw_ps_weights.weight)::NUMERIC(20, 4) AS weight
                        FROM edw_vw_ps_weights
                        WHERE (
                                (
                                    (
                                        (upper((edw_vw_ps_weights.kpi)::TEXT) = 'SOS COMPLIANCE'::TEXT)
                                        OR (upper((edw_vw_ps_weights.kpi)::TEXT) = 'SOA COMPLIANCE'::TEXT)
                                        )
                                    AND (upper((edw_vw_ps_weights.channel)::TEXT) = 'MODERN TRADE'::TEXT)
                                    )
                                AND (upper((edw_vw_ps_weights.market)::TEXT) = 'JAPAN'::TEXT)
                                )
                        ) kpi_wt ON (
                            (
                                (upper(vst.kpi) = upper((kpi_wt.kpi)::TEXT))
                                AND (upper((pop.retail_environment_ps)::TEXT) = upper((kpi_wt.retail_environment)::TEXT))
                                )
                            )
                    ) LEFT JOIN (
                    SELECT DISTINCT edw_vw_ps_targets.kpi,
                        edw_vw_ps_targets.attribute_1 AS category,
                        edw_vw_ps_targets.attribute_2 AS segment,
                        edw_vw_ps_targets.value AS "target"
                    FROM edw_vw_ps_targets
                    WHERE (
                            (
                                (
                                    (upper((edw_vw_ps_targets.kpi)::TEXT) = 'SOS COMPLIANCE'::TEXT)
                                    OR (upper((edw_vw_ps_targets.kpi)::TEXT) = 'SOA COMPLIANCE'::TEXT)
                                    )
                                AND (upper((edw_vw_ps_targets.channel)::TEXT) = 'MODERN TRADE'::TEXT)
                                )
                            AND (upper((edw_vw_ps_targets.market)::TEXT) = 'JAPAN'::TEXT)
                            )
                    ) mkt_share ON (
                        (
                            (
                                (upper(vst.kpi) = upper((mkt_share.kpi)::TEXT))
                                AND (upper((prd.ps_category)::TEXT) = upper((mkt_share.category)::TEXT))
                                )
                            AND (upper((prd.ps_segment)::TEXT) = upper((mkt_share.segment)::TEXT))
                            )
                        )
                )
            WHERE (date_part(year, (vst.visit_date)::TIMESTAMP without TIME zone) >= (date_part(year, convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)) - (2)::DOUBLE PRECISION))
            ),
        final AS (
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
            )
    SELECT *
    FROM final