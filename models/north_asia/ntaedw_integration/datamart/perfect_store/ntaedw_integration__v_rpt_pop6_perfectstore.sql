
with edw_vw_pop6_visits_sku_audits as 
(
    select * from {{ ref('ntaedw_integration__edw_vw_pop6_visits_sku_audits') }} 
),
edw_vw_pop6_store as 
(
    select * from {{ ref('ntaedw_integration__edw_vw_pop6_store') }}
),
edw_vw_ps_weights as 
(
    select * from {{ ref('aspedw_integration__edw_vw_ps_weights') }}
),
edw_vw_pop6_salesperson as 
(
    select * from {{ ref('ntaedw_integration__edw_vw_pop6_salesperson') }}
),
edw_vw_pop6_products as 
(
    select * from {{ ref('ntaedw_integration__edw_vw_pop6_products') }} 
),
edw_product_attr_dim as 
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
pop6_kpi2data_mapping as 
(
    select * from {{ source('ntaedw_integration','pop6_kpi2data_mapping') }}
),
edw_vw_pop6_visits_prod_attribute_audits as 
(
    select * from {{ ref('ntaedw_integration__edw_vw_pop6_visits_prod_attribute_audits') }}
),
edw_vw_ps_targets as 
(
    select * from {{ ref('aspedw_integration__edw_vw_ps_targets') }}
),
edw_vw_pop6_visits_display as 
(
    select * from {{ ref('ntaedw_integration__edw_vw_pop6_visits_display') }}
),
msl_compliance as 
(
    SELECT 
        'Merchandising_Response' AS dataset,
        null AS merchandisingresponseid,
        null AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        null AS mrch_resp_status,
        null AS mastersurveyid,
        null AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        null AS valuekey,
        vst.response AS value,
        vst.sku_id AS productid,
        'true' AS mustcarryitem,
        (
            CASE
                WHEN (
                    (upper((vst.response)::text) = 'YES'::text)
                    OR (upper((vst.response)::text) = (1)::text)
                ) THEN 'true'::text
                ELSE 'false'::text
            END
        )::character varying AS presence,
        null AS outofstock,
        'MSL Compliance' AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        datediff(
            second,
            (
                to_date(
                    (vst.check_in_datetime)::text
                )
            )::timestamp without time zone,
            (
                to_date(
                    (vst.check_out_datetime)::text
                )
            )::timestamp without time zone
        ) AS duration,
        (
            CASE
                WHEN (vst.cancelled_visit = 0) THEN 'completed'::text
                ELSE 'cancelled'::text
            END
        )::character varying AS vst_status,
        null AS fisc_yr,
        null AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS state,
        null AS county,
        null AS district,
        null AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        null AS soldtoparty,
        prd.sku_english AS productname,
        prd.barcode AS eannumber,
        prd.sap_matl_num AS matl_num,
        prd.country_l1 AS prod_hier_l1,
        prd.regional_franchise_l2 AS prod_hier_l2,
        prd.franchise_l3 AS prod_hier_l3,
        prd.brand_l4 AS prod_hier_l4,
        prd.sub_category_l5 AS prod_hier_l5,
        prd.platform_l6 AS prod_hier_l6,
        prd.variance_l7 AS prod_hier_l7,
        prd.pack_size_l8 AS prod_hier_l8,
        null AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        null AS mkt_share,
        null AS ques_desc,
        null AS "y/n_flag",
        null AS rej_reason,
        null AS response,
        null AS response_score,
        null AS acc_rej_reason
    FROM 
        edw_vw_pop6_visits_sku_audits vst
        LEFT JOIN 
        (
            edw_vw_pop6_store pop
            LEFT JOIN 
            (
                SELECT edw_vw_ps_weights.market AS ctry,
                    edw_vw_ps_weights.channel,
                    edw_vw_ps_weights.kpi AS kpi_name,
                    edw_vw_ps_weights.retail_environment AS store_type,
                    (edw_vw_ps_weights.weight)::double precision AS weight
                FROM edw_vw_ps_weights
                WHERE (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = 'MSL COMPLIANCE'::text
                        )
                        AND (
                            (
                                (
                                    upper((edw_vw_ps_weights.market)::text) = 'KOREA'::text
                                )
                                OR (
                                    upper((edw_vw_ps_weights.market)::text) = 'TAIWAN'::text
                                )
                            )
                            OR (
                                upper((edw_vw_ps_weights.market)::text) = 'HONG KONG'::text
                            )
                        )
                    )
            ) kpi_wt ON 
            (
                (
                    (
                        upper((pop.country)::text) = upper((kpi_wt.ctry)::text)
                    )
                    AND (
                        upper((pop.channel)::text) = upper((kpi_wt.channel)::text)
                    )
                    AND (
                        upper((pop.retail_environment_ps)::text) = upper((kpi_wt.store_type)::text)
                    )
                )
            )
        ) ON ((((vst.popdb_id)::text = (pop.popdb_id)::text)))
        LEFT JOIN edw_vw_pop6_salesperson usr ON ((((vst.username)::text = (usr.username)::text)))
        LEFT JOIN 
        (
            SELECT p.cntry_cd,
                p.src_file_date,
                p.status,
                p.productdb_id,
                p.barcode,
                p.sku,
                p.unit_price,
                p.display_order,
                p.launch_date,
                p.largest_uom_quantity,
                p.middle_uom_quantity,
                p.smallest_uom_quantity,
                p.company,
                p.sku_english,
                p.sku_code,
                p.ps_category,
                p.ps_segment,
                p.ps_category_segment,
                p.country_l1,
                p.regional_franchise_l2,
                p.franchise_l3,
                p.brand_l4,
                p.sub_category_l5,
                p.platform_l6,
                p.variance_l7,
                p.pack_size_l8,
                pa.sap_matl_num
            FROM (
                    edw_vw_pop6_products p
                    LEFT JOIN edw_product_attr_dim pa ON (
                        (
                            ((p.barcode)::text = (pa.ean)::text)
                            AND ((p.cntry_cd)::text = (pa.cntry)::text)
                        )
                    )
                )
        ) prd ON (
            (((vst.sku_id)::text = (prd.productdb_id)::text))
        )
    WHERE ((vst.field_code)::text = 'PS_MSL'::text)
),
oos_compliance as
(
    SELECT 
        'Merchandising_Response' AS dataset,
        null AS merchandisingresponseid,
        null AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        null AS mrch_resp_status,
        null AS mastersurveyid,
        null AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        null AS valuekey,
        vst.response AS value,
        vst.sku_id AS productid,
        'true' AS mustcarryitem,
        'true' AS presence,
        CASE
            WHEN (
                (vst_oos.field_code)::text = 'PS_MSL_OOS_Reason'::text
            ) THEN vst_oos.response
            ELSE NULL::character varying
        END AS outofstock,
        'OOS Compliance' AS kpi,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        datediff(
            second,
            (
                to_date(
                    (vst.check_in_datetime)::text
                )
            )::timestamp without time zone,
            (
                to_date(
                    (vst.check_out_datetime)::text
                )
            )::timestamp without time zone
        ) AS duration,
        (
            CASE
                WHEN (vst.cancelled_visit = 0) THEN 'completed'::text
                ELSE 'cancelled'::text
            END
        )::character varying AS vst_status,
        null AS fisc_yr,
        null AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS state,
        null AS county,
        null AS district,
        null AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        null AS soldtoparty,
        prd.sku_english AS productname,
        prd.barcode AS eannumber,
        prd.sap_matl_num AS matl_num,
        prd.country_l1 AS prod_hier_l1,
        prd.regional_franchise_l2 AS prod_hier_l2,
        prd.franchise_l3 AS prod_hier_l3,
        prd.brand_l4 AS prod_hier_l4,
        prd.sub_category_l5 AS prod_hier_l5,
        prd.platform_l6 AS prod_hier_l6,
        prd.variance_l7 AS prod_hier_l7,
        prd.pack_size_l8 AS prod_hier_l8,
        null AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        null AS mkt_share,
        null AS ques_desc,
        null AS "y/n_flag",
        null AS rej_reason,
        null AS response,
        null AS response_score,
        null AS acc_rej_reason
    FROM 
        edw_vw_pop6_visits_sku_audits vst
        LEFT JOIN 
        (
            edw_vw_pop6_store pop
            LEFT JOIN (
                SELECT edw_vw_ps_weights.market AS ctry,
                    edw_vw_ps_weights.channel,
                    edw_vw_ps_weights.kpi AS kpi_name,
                    edw_vw_ps_weights.retail_environment AS store_type,
                    (edw_vw_ps_weights.weight)::double precision AS weight
                FROM edw_vw_ps_weights
                WHERE (
                        (
                            upper((edw_vw_ps_weights.kpi)::text) = 'OOS COMPLIANCE'::text
                        )
                        AND (
                            (
                                (
                                    upper((edw_vw_ps_weights.market)::text) = 'KOREA'::text
                                )
                                OR (
                                    upper((edw_vw_ps_weights.market)::text) = 'TAIWAN'::text
                                )
                            )
                            OR (
                                upper((edw_vw_ps_weights.market)::text) = 'HONG KONG'::text
                            )
                        )
                    )
            ) kpi_wt ON (
                (
                    (
                        upper((pop.country)::text) = upper((kpi_wt.ctry)::text)
                    )
                    AND (
                        upper((pop.channel)::text) = upper((kpi_wt.channel)::text)
                    )
                    AND (
                        upper((pop.retail_environment_ps)::text) = upper((kpi_wt.store_type)::text)
                    )
                )
            )
        ) ON ((((vst.popdb_id)::text = (pop.popdb_id)::text)))
        LEFT JOIN edw_vw_pop6_salesperson usr ON ((((vst.username)::text = (usr.username)::text)))
        LEFT JOIN 
        (
            SELECT 
                p.cntry_cd,
                p.src_file_date,
                p.status,
                p.productdb_id,
                p.barcode,
                p.sku,
                p.unit_price,
                p.display_order,
                p.launch_date,
                p.largest_uom_quantity,
                p.middle_uom_quantity,
                p.smallest_uom_quantity,
                p.company,
                p.sku_english,
                p.sku_code,
                p.ps_category,
                p.ps_segment,
                p.ps_category_segment,
                p.country_l1,
                p.regional_franchise_l2,
                p.franchise_l3,
                p.brand_l4,
                p.sub_category_l5,
                p.platform_l6,
                p.variance_l7,
                p.pack_size_l8,
                pa.sap_matl_num
            FROM (
                    edw_vw_pop6_products p
                    LEFT JOIN edw_product_attr_dim pa ON (
                        (
                            ((p.barcode)::text = (pa.ean)::text)
                            AND ((p.cntry_cd)::text = (pa.cntry)::text)
                        )
                    )
                )
        ) prd ON (
            (((vst.sku_id)::text = (prd.productdb_id)::text))
        )
        LEFT JOIN 
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
            WHERE (
                    (edw_vw_pop6_visits_sku_audits.field_code)::text = 'PS_MSL_OOS_Reason'::text
                )
        ) vst_oos ON 
        (
            (
                ((vst.visit_id)::text = (vst_oos.visit_id)::text)
                AND ((vst.sku_id)::text = (vst_oos.sku_id)::text)
                AND (
                    (vst.field_id)::text = (vst_oos.dependent_on_field_id)::text
                )
            )
        )
    WHERE ((vst.field_code)::text = 'PS_MSL_OOS'::text)
),
srv_1 as 
(
    SELECT 
        'Survey_Response'::character varying AS dataset,
        NULL::character varying AS merchandisingresponseid,
        NULL::character varying AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL::character varying AS mrch_resp_status,
        NULL::character varying AS mastersurveyid,
        NULL::character varying AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL::character varying AS valuekey,
        vst.response AS value,
        NULL::character varying AS productid,
        NULL::character varying AS mustcarryitem,
        NULL::character varying AS presence,
        NULL::character varying AS outofstock,
        mapp.kpi_name,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        datediff(
            second,
            (
                to_date(
                    (vst.check_in_datetime)::text
                )
            )::timestamp without time zone,
            (
                to_date(
                    (vst.check_out_datetime)::text
                )
            )::timestamp without time zone
        ) AS duration,
        CASE
            WHEN (vst.cancelled_visit = 0) THEN 'completed'::text
            ELSE 'cancelled'::text
        END AS vst_status,
        NULL::character varying AS fisc_yr,
        NULL::character varying AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS state,
        NULL::character varying AS county,
        NULL::character varying AS district,
        NULL::character varying AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL::character varying AS soldtoparty,
        NULL::character varying AS productname,
        NULL::character varying AS eannumber,
        NULL::character varying AS matl_num,
        NULL::character varying AS prod_hier_l1,
        NULL::character varying AS prod_hier_l2,
        NULL::character varying AS prod_hier_l3,
        NULL::character varying AS prod_hier_l4,
        NULL::character varying AS prod_hier_l5,
        NULL::character varying AS prod_hier_l6,
        NULL::character varying AS prod_hier_l7,
        NULL::character varying AS prod_hier_l8,
        NULL::character varying AS prod_hier_l9
    FROM (
            SELECT pop6_kpi2data_mapping.ctry,
                pop6_kpi2data_mapping.identifier,
                pop6_kpi2data_mapping.kpi_name
            FROM pop6_kpi2data_mapping
            WHERE (
                    (pop6_kpi2data_mapping.data_type)::text = 'KPI'::text
                )
        ) mapp,
        (
            (
                (
                    edw_vw_pop6_visits_prod_attribute_audits vst
                    LEFT JOIN edw_vw_pop6_store pop ON ((((vst.popdb_id)::text = (pop.popdb_id)::text)))
                )
                LEFT JOIN edw_vw_pop6_salesperson usr ON ((((vst.username)::text = (usr.username)::text)))
            )
            LEFT JOIN (
                SELECT DISTINCT NULL::text AS brand_l4,
                    edw_vw_pop6_products.ps_category,
                    edw_vw_pop6_products.ps_segment,
                    (
                        (
                            (
                                (edw_vw_pop6_products.ps_category)::text || '_'::text
                            ) || (edw_vw_pop6_products.ps_segment)::text
                        )
                    )::character varying AS ps_categorysegement
                FROM edw_vw_pop6_products
                UNION
                SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                    edw_vw_pop6_products.ps_category,
                    edw_vw_pop6_products.ps_segment,
                    NULL::text AS ps_categorysegement
                FROM edw_vw_pop6_products
            ) prd ON (
                (
                    CASE
                        WHEN (
                            (vst.product_attribute)::text = 'PS Category Segment'::text
                        ) THEN (
                            (vst.product_attribute_value)::text = (prd.ps_categorysegement)::text
                        )
                        ELSE (
                            (vst.product_attribute_value)::text = prd.brand_l4
                        )
                    END
                )
            )
        )
    WHERE 
        (
            (
                (
                    "substring"((vst.field_code)::text, 1, 6) = (mapp.identifier)::text
                )
                AND ((pop.country)::text = (mapp.ctry)::text)
            )
            AND (
                (
                    (
                        "substring"((vst.field_code)::text, 1, 6) = 'PS_POG'::text
                    )
                    OR (
                        "substring"((vst.field_code)::text, 1, 6) = 'PS_SOS'::text
                    )
                )
                OR (
                    "substring"((vst.field_code)::text, 1, 6) = 'PS_SOA'::text
                )
            )
        )
),
survey_response_1 as 
(
    SELECT 
        srv.dataset,
        srv.merchandisingresponseid,
        srv.surveyresponseid,
        srv.customerid,
        srv.salespersonid,
        srv.visitid,
        srv.mrch_resp_startdt,
        srv.mrch_resp_enddt,
        srv.mrch_resp_status,
        srv.mastersurveyid,
        srv.survey_status,
        srv.survey_enddate,
        srv.questionkey,
        srv.questiontext,
        srv.valuekey,
        srv.value,
        srv.productid,
        srv.mustcarryitem,
        srv.presence,
        srv.outofstock,
        srv.kpi_name AS kpi,
        srv.category,
        srv.segment,
        srv.vst_visitid,
        srv.scheduleddate,
        srv.scheduledtime,
        srv.duration,
        srv.vst_status,
        srv.fisc_yr,
        srv.fisc_per,
        srv.firstname,
        srv.lastname,
        srv.cust_remotekey,
        srv.customername,
        srv.country,
        srv.state,
        srv.county,
        srv.district,
        srv.city,
        srv.storereference,
        srv.storetype,
        srv.channel,
        srv.salesgroup,
        srv.bu,
        srv.soldtoparty,
        srv.productname,
        srv.eannumber,
        srv.matl_num,
        srv.prod_hier_l1,
        srv.prod_hier_l2,
        srv.prod_hier_l3,
        srv.prod_hier_l4,
        srv.prod_hier_l5,
        srv.prod_hier_l6,
        srv.prod_hier_l7,
        srv.prod_hier_l8,
        srv.prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        mkt_shr.mkt_share,
        ques.value AS ques_desc,
        flag.value AS "y/n_flag",
        null AS rej_reason,
        null AS response,
        null AS response_score,
        null AS acc_rej_reason
    FROM srv_1 as srv
    LEFT JOIN 
    (
        SELECT edw_vw_ps_weights.market AS ctry,
            edw_vw_ps_weights.channel,
            edw_vw_ps_weights.kpi AS kpi_name,
            edw_vw_ps_weights.retail_environment AS store_type,
            (edw_vw_ps_weights.weight)::double precision AS weight
        FROM edw_vw_ps_weights
        WHERE (
                (
                    (
                        upper((edw_vw_ps_weights.market)::text) = 'KOREA'::text
                    )
                    OR (
                        upper((edw_vw_ps_weights.market)::text) = 'TAIWAN'::text
                    )
                )
                OR (
                    upper((edw_vw_ps_weights.market)::text) = 'HONG KONG'::text
                )
            )
    ) kpi_wt ON 
    (
        (
            (
                (
                    (
                        upper((kpi_wt.kpi_name)::text) = upper((srv.kpi_name)::text)
                    )
                    AND (
                        upper((kpi_wt.ctry)::text) = upper((srv.country)::text)
                    )
                )
                AND (
                    upper((kpi_wt.channel)::text) = upper((srv.channel)::text)
                )
            )
            AND (
                upper((kpi_wt.store_type)::text) = upper((srv.storetype)::text)
            )
        )
    )
    LEFT JOIN 
    (
        SELECT pop6_kpi2data_mapping.ctry,
            pop6_kpi2data_mapping.data_type,
            pop6_kpi2data_mapping.identifier AS ques_identifier,
            pop6_kpi2data_mapping.value,
            pop6_kpi2data_mapping.kpi_name,
            pop6_kpi2data_mapping.start_date,
            pop6_kpi2data_mapping.end_date
        FROM pop6_kpi2data_mapping
        WHERE (
                (pop6_kpi2data_mapping.data_type)::text = 'Question'::text
            )
    ) ques ON 
    (
        (
            (
                (
                    (
                        ((ques.ctry)::text = (srv.country)::text)
                        AND ((ques.kpi_name)::text = (srv.kpi_name)::text)
                    )
                    AND (
                        trim((srv.questionkey)::text) = (ques.ques_identifier)::text
                    )
                )
                AND (srv.scheduleddate >= ques.start_date)
            )
            AND (srv.scheduleddate <= ques.end_date)
        )
    )
    LEFT JOIN 
    (
        SELECT pop6_kpi2data_mapping.ctry,
            pop6_kpi2data_mapping.data_type,
            pop6_kpi2data_mapping.identifier,
            pop6_kpi2data_mapping.value,
            pop6_kpi2data_mapping.kpi_name,
            pop6_kpi2data_mapping.start_date,
            pop6_kpi2data_mapping.end_date
        FROM pop6_kpi2data_mapping
        WHERE (
                (pop6_kpi2data_mapping.data_type)::text = 'Yes/No Flag'::text
            )
    ) flag ON 
    (
        (
            (
                (
                    (
                        ((flag.ctry)::text = (srv.country)::text)
                        AND ((flag.kpi_name)::text = (srv.kpi_name)::text)
                    )
                    AND (
                        upper((srv.value)::text) like (
                            ('%'::text || (flag.identifier)::text) || '%'::text
                        )
                    )
                )
                AND (srv.scheduleddate >= flag.start_date)
            )
            AND (srv.scheduleddate <= flag.end_date)
        )
    )
    LEFT JOIN 
    (
        SELECT edw_vw_ps_targets.market AS ctry,
            edw_vw_ps_targets.channel,
            edw_vw_ps_targets.kpi AS kpi_name,
            edw_vw_ps_targets.retail_environment AS store_type,
            edw_vw_ps_targets.attribute_1 AS category,
            edw_vw_ps_targets.attribute_2 AS segment,
            (edw_vw_ps_targets.value)::double precision AS mkt_share
        FROM edw_vw_ps_targets
        WHERE (
                (
                    (
                        upper((edw_vw_ps_targets.market)::text) = 'KOREA'::text
                    )
                    OR (
                        upper((edw_vw_ps_targets.market)::text) = 'TAIWAN'::text
                    )
                )
                OR (
                    upper((edw_vw_ps_targets.market)::text) = 'HONG KONG'::text
                )
            )
    ) mkt_shr ON 
    (
        (
            (
                (
                    (
                        (
                            (
                                upper((mkt_shr.kpi_name)::text) = upper((srv.kpi_name)::text)
                            )
                            AND (
                                upper((mkt_shr.ctry)::text) = upper((srv.country)::text)
                            )
                        )
                        AND (
                            upper((mkt_shr.channel)::text) = upper((srv.channel)::text)
                        )
                    )
                    AND (
                        upper((mkt_shr.store_type)::text) = upper((srv.storetype)::text)
                    )
                )
                AND (
                    upper((mkt_shr.category)::text) = upper((srv.category)::text)
                )
            )
            AND (
                upper((mkt_shr.segment)::text) = upper((srv.segment)::text)
            )
        )
    )
),
srv_2 as
(
    SELECT 
        'Survey_Response'::character varying AS dataset,
        NULL::character varying AS merchandisingresponseid,
        NULL::character varying AS surveyresponseid,
        pop.popdb_id AS customerid,
        usr.userdb_id AS salespersonid,
        vst.visit_id AS visitid,
        vst.check_in_datetime AS mrch_resp_startdt,
        vst.check_out_datetime AS mrch_resp_enddt,
        NULL::character varying AS mrch_resp_status,
        NULL::character varying AS mastersurveyid,
        NULL::character varying AS survey_status,
        vst.check_out_datetime AS survey_enddate,
        vst.field_code AS questionkey,
        vst.field_label AS questiontext,
        NULL::character varying AS valuekey,
        CASE
            WHEN (
                ((pop.country)::text = 'Korea'::text)
                AND (upper((rej.response)::text) like 'N.A%'::text)
            ) THEN ' '::character varying
            WHEN (vst.dependent_on_field_id IS NOT NULL) THEN ' '::character varying
            ELSE vst.response
        END AS value,
        NULL::character varying AS productid,
        NULL::character varying AS mustcarryitem,
        NULL::character varying AS presence,
        NULL::character varying AS outofstock,
        mapp.kpi_name,
        prd.ps_category AS category,
        prd.ps_segment AS segment,
        vst.visit_id AS vst_visitid,
        vst.visit_date AS scheduleddate,
        vst.check_in_datetime AS scheduledtime,
        datediff(
            second,
            (
                to_date(
                    (vst.check_in_datetime)::text
                )
            )::timestamp without time zone,
            (
                to_date(
                    (vst.check_out_datetime)::text
                )
            )::timestamp without time zone
        ) AS duration,
        CASE
            WHEN (vst.cancelled_visit = 0) THEN 'completed'::text
            ELSE 'cancelled'::text
        END AS vst_status,
        NULL::character varying AS fisc_yr,
        NULL::character varying AS fisc_per,
        usr.first_name AS firstname,
        usr.last_name AS lastname,
        pop.popdb_id AS cust_remotekey,
        pop.pop_name AS customername,
        pop.country,
        pop.territory_or_region AS state,
        NULL::character varying AS county,
        NULL::character varying AS district,
        NULL::character varying AS city,
        pop.customer AS storereference,
        pop.retail_environment_ps AS storetype,
        pop.channel,
        pop.sales_group_name AS salesgroup,
        pop.business_unit_name AS bu,
        NULL::character varying AS soldtoparty,
        NULL::character varying AS productname,
        NULL::character varying AS eannumber,
        NULL::character varying AS matl_num,
        NULL::character varying AS prod_hier_l1,
        NULL::character varying AS prod_hier_l2,
        NULL::character varying AS prod_hier_l3,
        NULL::character varying AS prod_hier_l4,
        NULL::character varying AS prod_hier_l5,
        NULL::character varying AS prod_hier_l6,
        NULL::character varying AS prod_hier_l7,
        NULL::character varying AS prod_hier_l8,
        NULL::character varying AS prod_hier_l9,
        CASE
            WHEN (
                (upper((vst.response)::text) = 'YES'::text)
                OR (upper((vst.response)::text) = '1'::text)
            ) THEN ' '::character varying
            WHEN (
                (upper((vst.response)::text) = 'NO'::text)
                OR (upper((vst.response)::text) = '0'::text)
            ) THEN rej.response
            ELSE vst.response
        END AS rej_reason,
        CASE
            WHEN ((pop.country)::text = 'Taiwan'::text) THEN CASE
                WHEN (
                    (upper((vst.response)::text) = '0'::text)
                    OR (
                        (upper((vst.response)::text) IS NULL)
                        AND ('0' IS NULL)
                    )
                ) THEN 'NO'::text
                WHEN (
                    (upper((vst.response)::text) = '1'::text)
                    OR (
                        (upper((vst.response)::text) IS NULL)
                        AND ('1' IS NULL)
                    )
                ) THEN 'YES'::text
                ELSE upper((vst.response)::text)
            END
            ELSE ' '::text
        END AS response,
        CASE
            WHEN (
                ((pop.country)::text = 'Taiwan'::text)
                AND (
                    (upper((vst.response)::text) = 'YES'::text)
                    OR (upper((vst.response)::text) = '1'::text)
                )
            ) THEN trim(
                split_part(trim((rej.response)::text), ','::text, 1)
            )
            WHEN (
                ((pop.country)::text = 'Taiwan'::text)
                AND (
                    (upper((vst.response)::text) = 'NO'::text)
                    OR (upper((vst.response)::text) = '0'::text)
                )
            ) THEN '0'::text
            ELSE ' '::text
        END AS response_score,
        CASE
            WHEN (
                ((pop.country)::text = 'Taiwan'::text)
                AND (
                    (upper((vst.response)::text) = 'YES'::text)
                    OR (upper((vst.response)::text) = '1'::text)
                )
            ) THEN trim(
                split_part(trim((rej.response)::text), ','::text, 2)
            )
            WHEN (
                ((pop.country)::text = 'Taiwan'::text)
                AND (
                    (upper((vst.response)::text) = 'NO'::text)
                    OR (upper((vst.response)::text) = '0'::text)
                )
            ) THEN (rej.response)::text
            ELSE ' '::text
        END AS acc_rej_reason
    FROM (
            SELECT DISTINCT pop6_kpi2data_mapping.ctry,
                pop6_kpi2data_mapping.identifier,
                pop6_kpi2data_mapping.kpi_name
            FROM pop6_kpi2data_mapping
            WHERE (
                    (pop6_kpi2data_mapping.data_type)::text = 'KPI'::text
                )
        ) mapp,
        (
            (
                (
                    (
                        edw_vw_pop6_visits_display vst
                        LEFT JOIN edw_vw_pop6_store pop ON ((((vst.popdb_id)::text = (pop.popdb_id)::text)))
                    )
                    LEFT JOIN edw_vw_pop6_salesperson usr ON ((((vst.username)::text = (usr.username)::text)))
                )
                LEFT JOIN (
                    SELECT DISTINCT NULL::text AS brand_l4,
                        edw_vw_pop6_products.ps_category,
                        edw_vw_pop6_products.ps_segment,
                        (
                            (
                                (
                                    (edw_vw_pop6_products.ps_category)::text || '_'::text
                                ) || (edw_vw_pop6_products.ps_segment)::text
                            )
                        )::character varying AS ps_categorysegement
                    FROM edw_vw_pop6_products
                    UNION
                    SELECT DISTINCT edw_vw_pop6_products.brand_l4,
                        edw_vw_pop6_products.ps_category,
                        edw_vw_pop6_products.ps_segment,
                        NULL::text AS ps_categorysegement
                    FROM edw_vw_pop6_products
                ) prd ON (
                    (
                        CASE
                            WHEN (
                                (vst.product_attribute)::text = 'PS Category Segment'::text
                            ) THEN (
                                (vst.product_attribute_value)::text = (prd.ps_categorysegement)::text
                            )
                            ELSE (
                                (vst.product_attribute_value)::text = prd.brand_l4
                            )
                        END
                    )
                )
            )
            LEFT JOIN (
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
                        upper((edw_vw_pop6_visits_display.field_type)::text) <> 'PHOTO'::text
                    )
            ) rej ON (
                (
                    ((vst.visit_id)::text = (rej.visit_id)::text)
                    AND (
                        (vst.product_attribute_value)::text = (rej.product_attribute_value)::text
                    )
                    AND (
                        (vst.field_id)::text = (rej.dependent_on_field_id)::text
                    )
                )
            )
        )
    WHERE (
            (
                ((pop.country)::text = (mapp.ctry)::text)
                AND (
                    (vst.field_code)::text like (
                        ('%'::text || (mapp.identifier)::text) || '%'::text
                    )
                )
            )
            AND (
                (
                    (
                        (
                            ((vst.field_code)::text <> 'DIS_NC_REASON'::text)
                            AND ((vst.field_code)::text <> 'Photo_Display'::text)
                        )
                        AND ((vst.field_code)::text <> 'Photo_Promo'::text)
                    )
                    AND ((vst.cntry_cd)::text = 'KR'::text)
                )
                OR (
                    (
                        ((vst.field_type)::text = 'Yes/No'::text)
                        OR ((vst.field_type)::text = 'Yes_No'::text)
                    )
                    AND ((vst.cntry_cd)::text <> 'KR'::text)
                )
            )
        )
),
survey_response_2 as
(
    SELECT 
        srv.dataset,
        srv.merchandisingresponseid,
        srv.surveyresponseid,
        srv.customerid,
        srv.salespersonid,
        srv.visitid,
        srv.mrch_resp_startdt,
        srv.mrch_resp_enddt,
        srv.mrch_resp_status,
        srv.mastersurveyid,
        srv.survey_status,
        srv.survey_enddate,
        srv.questionkey,
        srv.questiontext,
        srv.valuekey,
        srv.value,
        srv.productid,
        srv.mustcarryitem,
        srv.presence,
        srv.outofstock,
        srv.kpi_name AS kpi,
        srv.category,
        srv.segment,
        srv.vst_visitid,
        srv.scheduleddate,
        srv.scheduledtime,
        srv.duration,
        (srv.vst_status)::character varying AS vst_status,
        srv.fisc_yr,
        srv.fisc_per,
        srv.firstname,
        srv.lastname,
        srv.cust_remotekey,
        srv.customername,
        srv.country,
        srv.state,
        srv.county,
        srv.district,
        srv.city,
        srv.storereference,
        srv.storetype,
        srv.channel,
        srv.salesgroup,
        srv.bu,
        srv.soldtoparty,
        srv.productname,
        srv.eannumber,
        srv.matl_num,
        srv.prod_hier_l1,
        srv.prod_hier_l2,
        srv.prod_hier_l3,
        srv.prod_hier_l4,
        srv.prod_hier_l5,
        srv.prod_hier_l6,
        srv.prod_hier_l7,
        srv.prod_hier_l8,
        srv.prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        mkt_shr.mkt_share,
        ques.value AS ques_desc,
        flag.value AS "y/n_flag",
        srv.rej_reason,
        (srv.response)::character varying AS response,
        (srv.response_score)::character varying AS response_score,
        (srv.acc_rej_reason)::character varying AS acc_rej_reason
    FROM srv_2 srv
        LEFT JOIN 
        (
            SELECT edw_vw_ps_weights.market AS ctry,
                edw_vw_ps_weights.channel,
                edw_vw_ps_weights.kpi AS kpi_name,
                edw_vw_ps_weights.retail_environment AS store_type,
                (edw_vw_ps_weights.weight)::double precision AS weight
            FROM edw_vw_ps_weights
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_weights.market)::text) = 'KOREA'::text
                        )
                        OR (
                            upper((edw_vw_ps_weights.market)::text) = 'TAIWAN'::text
                        )
                    )
                    OR (
                        upper((edw_vw_ps_weights.market)::text) = 'HONG KONG'::text
                    )
                )
        ) kpi_wt ON 
        (
            (
                (
                    (
                        (
                            upper((kpi_wt.kpi_name)::text) = upper((srv.kpi_name)::text)
                        )
                        AND (
                            upper((kpi_wt.ctry)::text) = upper((srv.country)::text)
                        )
                    )
                    AND (
                        upper((kpi_wt.channel)::text) = upper((srv.channel)::text)
                    )
                )
                AND (
                    upper((kpi_wt.store_type)::text) = upper((srv.storetype)::text)
                )
            )
        )
        LEFT JOIN 
        (
            SELECT pop6_kpi2data_mapping.ctry,
                pop6_kpi2data_mapping.data_type,
                pop6_kpi2data_mapping.identifier AS ques_identifier,
                pop6_kpi2data_mapping.value,
                pop6_kpi2data_mapping.kpi_name,
                pop6_kpi2data_mapping.start_date,
                pop6_kpi2data_mapping.end_date
            FROM pop6_kpi2data_mapping
            WHERE (
                    (pop6_kpi2data_mapping.data_type)::text = 'Question'::text
                )
        ) ques ON 
        (
            (
                (
                    (
                        (
                            ((ques.ctry)::text = (srv.country)::text)
                            AND ((ques.kpi_name)::text = (srv.kpi_name)::text)
                        )
                        AND (
                            (srv.questiontext)::text like (
                                ('%'::text || (ques.ques_identifier)::text) || '%'::text
                            )
                        )
                    )
                    AND (srv.scheduleddate >= ques.start_date)
                )
                AND (srv.scheduleddate <= ques.end_date)
            )
        )
        LEFT JOIN 
        (
            SELECT pop6_kpi2data_mapping.ctry,
                pop6_kpi2data_mapping.data_type,
                pop6_kpi2data_mapping.identifier,
                pop6_kpi2data_mapping.value,
                pop6_kpi2data_mapping.kpi_name,
                pop6_kpi2data_mapping.start_date,
                pop6_kpi2data_mapping.end_date
            FROM pop6_kpi2data_mapping
            WHERE (
                    (pop6_kpi2data_mapping.data_type)::text = 'Yes/No Flag'::text
                )
        ) flag ON 
        (
            (
                (
                    (
                        (
                            ((flag.ctry)::text = (srv.country)::text)
                            AND ((flag.kpi_name)::text = (srv.kpi_name)::text)
                        )
                        AND (
                            upper((srv.value)::text) like (
                                ('%'::text || (flag.identifier)::text) || '%'::text
                            )
                        )
                    )
                    AND (srv.scheduleddate >= flag.start_date)
                )
                AND (srv.scheduleddate <= flag.end_date)
            )
        )
        LEFT JOIN 
        (
            SELECT edw_vw_ps_targets.market AS ctry,
                edw_vw_ps_targets.channel,
                edw_vw_ps_targets.kpi AS kpi_name,
                edw_vw_ps_targets.retail_environment AS store_type,
                edw_vw_ps_targets.attribute_1 AS category,
                edw_vw_ps_targets.attribute_2 AS segment,
                (edw_vw_ps_targets.value)::double precision AS mkt_share
            FROM edw_vw_ps_targets
            WHERE (
                    (
                        (
                            upper((edw_vw_ps_targets.market)::text) = 'KOREA'::text
                        )
                        OR (
                            upper((edw_vw_ps_targets.market)::text) = 'TAIWAN'::text
                        )
                    )
                    OR (
                        upper((edw_vw_ps_targets.market)::text) = 'HONG KONG'::text
                    )
                )
        ) mkt_shr ON 
        (
            (
                (
                    (
                        (
                            (
                                ((mkt_shr.kpi_name)::text = (srv.kpi_name)::text)
                                AND ((mkt_shr.ctry)::text = (srv.country)::text)
                            )
                            AND ((mkt_shr.channel)::text = (srv.channel)::text)
                        )
                        AND (
                            upper((mkt_shr.store_type)::text) = upper((srv.storetype)::text)
                        )
                    )
                    AND (
                        upper((mkt_shr.category)::text) = upper((srv.category)::text)
                    )
                )
                AND (
                    upper((mkt_shr.segment)::text) = upper((srv.segment)::text)
                )
            )
        )
),
final as
(
    select * from msl_compliance
    union all
    select * from oos_compliance
    union all
    select * from survey_response_1
    union all
    select * from survey_response_2
)
select * from final