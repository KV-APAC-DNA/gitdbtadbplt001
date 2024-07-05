with v_rpt_rex_perfect_store as (
    select * from aspedw_integration.v_rpt_rex_perfect_store
),
edw_vw_pop6_products as (
    select * from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
),
v_rpt_pop6_perfectstore as (
    select * from {{ ref('aspedw_integration__v_rpt_pop6_perfectstore') }}
),
itg_query_parameters as (
    select * from {{ source('aspitg_integration','itg_query_parameters') }}
),
edw_pacific_perfect_store as (
    select * from {{ ref('pcfedw_integration__edw_pacific_perfect_store') }}
),
v_rpt_ph_perfect_store as (
    select * from {{ ref('phledw_integration__v_rpt_ph_perfect_store') }}
),
v_rpt_id_regional_perfect_store as (
    select * from idnedw_integration.v_rpt_id_regional_perfect_store
),
v_rpt_jp_pop6_perfect_store as (
    select * from {{ ref('ntaedw_integration__v_rpt_jp_pop6_perfect_store') }}
),
v_rpt_sg_pop6_perfect_store as (
    select * from {{ ref('sgpedw_integration__v_rpt_sg_pop6_perfect_store') }}
),
v_rpt_th_pop6_perfect_store as (
    select * from {{ ref('ntaedw_integration__v_rpt_th_pop6_perfect_store') }}
),
v_rpt_my_perfect_store_snapshot as (
    select * from mysedw_integration.v_rpt_my_perfect_store_snapshot
),
v_rpt_my_perfect_store as (
    select * from {{ ref('mysedw_integration__v_rpt_my_perfect_store') }}
),
edw_sku_recom as (
    select * from indedw_integration.edw_sku_recom
),
itg_udcdetails as (
    select * from inditg_integration.itg_udcdetails
),
edw_calendar_dim as (
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_sku_recom as (
    select * from indedw_integration.edw_sku_recom
),
edw_vw_ps_weights as (
    select * from {{ ref('aspedw_integration__edw_vw_ps_weights') }}
),
itg_in_perfectstore_msl as (
    select * from inditg_integration.itg_in_perfectstore_msl
),
edw_product_key_attributes as (
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
itg_in_perfectstore_sos as (
    select * from inditg_integration.itg_in_perfectstore_sos
),
edw_vw_ps_targets as (
    select * from {{ ref('aspedw_integration__edw_vw_ps_targets') }}
),
itg_in_perfectstore_promo as (
    select * from inditg_integration.itg_in_perfectstore_promo
),
itg_in_perfectstore_paid_display as (
    select * from inditg_integration.itg_in_perfectstore_paid_display
),
rpt_in_perfect_store as (
    select * from indedw_integration.rpt_in_perfect_store
),
edw_rpt_th_perfect_store as (
    select * from {{ ref('thaedw_integration__edw_rpt_th_perfect_store') }}
),
v_rpt_vn_perfect_store as (
    select * from {{ ref('vnmedw_integration__v_rpt_vn_perfect_store') }}
),
rex as (
    SELECT UPPER(derived_table1.dataset)::CHARACTER VARYING AS dataset,
    derived_table1.merchandisingresponseid,
    derived_table1.surveyresponseid,
    derived_table1.customerid,
    derived_table1.salespersonid,
    derived_table1.visitid,
    derived_table1.mrch_resp_startdt,
    derived_table1.mrch_resp_enddt,
    derived_table1.mrch_resp_status,
    derived_table1.mastersurveyid,
    derived_table1.survey_status,
    derived_table1.survey_enddate,
    derived_table1.questionkey,
    derived_table1.questiontext,
    derived_table1.valuekey,
    derived_table1.value,
    derived_table1.productid,
    UPPER(derived_table1.mustcarryitem)::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(derived_table1.presence)::CHARACTER VARYING AS presence,
    derived_table1.outofstock,
    derived_table1.mastersurveyname,
    UPPER(derived_table1.kpi)::CHARACTER VARYING AS kpi,
    derived_table1.category,
    derived_table1.segment,
    derived_table1.vst_visitid,
    derived_table1.scheduleddate,
    derived_table1.scheduledtime,
    derived_table1.duration,
    UPPER(derived_table1.vst_status)::CHARACTER VARYING AS vst_status,
    derived_table1.fisc_yr,
    derived_table1.fisc_per,
    derived_table1.firstname,
    derived_table1.lastname,
    derived_table1.cust_remotekey,
    derived_table1.customername,
    derived_table1.country,
    NULL AS state,
    derived_table1.county,
    derived_table1.district,
    derived_table1.city,
    derived_table1.storereference,
    derived_table1.storetype,
    derived_table1.channel,
    derived_table1.salesgroup,
    NULL AS bu,
    derived_table1.soldtoparty,
    NULL AS brand,
    derived_table1.productname,
    derived_table1.eannumber,
    derived_table1.matl_num,
    derived_table1.prod_hier_l1,
    derived_table1.prod_hier_l2,
    derived_table1.prod_hier_l3,
    derived_table1.prod_hier_l4,
    derived_table1.prod_hier_l5,
    derived_table1.prod_hier_l6,
    derived_table1.prod_hier_l7,
    derived_table1.prod_hier_l8,
    derived_table1.prod_hier_l9,
    derived_table1.kpi_chnl_wt,
    derived_table1.mkt_share,
    UPPER(derived_table1.ques_desc)::CHARACTER VARYING AS ques_desc,
    derived_table1."y/n_flag",
    NULL AS posm_execution_flag,
    derived_table1.rej_reason,
    derived_table1.response,
    derived_table1.response_score,
    derived_table1.acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    store_grade
FROM (
        SELECT UPPER(rex.dataset::TEXT) AS dataset,
            rex.merchandisingresponseid,
            rex.surveyresponseid,
            rex.customerid,
            rex.salespersonid,
            rex.visitid,
            rex.mrch_resp_startdt,
            rex.mrch_resp_enddt,
            rex.mrch_resp_status,
            rex.mastersurveyid,
            rex.survey_status,
            rex.survey_enddate,
            rex.questionkey,
            rex.questiontext,
            rex.valuekey,
            rex.value,
            rex.productid,
            UPPER(rex.mustcarryitem::TEXT) AS mustcarryitem,
            NULL::CHARACTER VARYING AS answerscore,
            UPPER(rex.presence::TEXT) AS presence,
            rex.outofstock,
            rex.mastersurveyname,
            UPPER(rex.kpi::TEXT) AS kpi,
            rex.category,
            rex.segment,
            rex.vst_visitid,
            rex.scheduleddate,
            rex.scheduledtime,
            rex.duration,
            UPPER(rex.vst_status::TEXT) AS vst_status,
            rex.fisc_yr,
            rex.fisc_per,
            rex.firstname,
            rex.lastname,
            rex.cust_remotekey,
            rex.customername,
            rex.country,
            NULL::CHARACTER VARYING AS state,
            rex.county,
            rex.district,
            rex.city,
            rex.storereference,
            rex.storetype,
            rex.channel,
            rex.salesgroup,
            rex.soldtoparty,
            NULL::CHARACTER VARYING AS brand,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.sku_english
                ELSE rex.productname
            END AS productname,
            rex.eannumber,
            rex.matl_num,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.country_l1
                ELSE rex.prod_hier_l1
            END AS prod_hier_l1,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.regional_franchise_l2
                ELSE rex.prod_hier_l2
            END AS prod_hier_l2,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.franchise_l3
                ELSE rex.prod_hier_l3
            END AS prod_hier_l3,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.brand_l4
                ELSE rex.prod_hier_l4
            END AS prod_hier_l4,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.sub_category_l5
                ELSE rex.prod_hier_l5
            END AS prod_hier_l5,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.platform_l6
                ELSE rex.prod_hier_l6
            END AS prod_hier_l6,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.variance_l7
                ELSE rex.prod_hier_l7
            END AS prod_hier_l7,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.pack_size_l8
                ELSE rex.prod_hier_l8
            END AS prod_hier_l8,
            CASE
                WHEN prods.sku_english IS NOT NULL THEN prods.sku_english
                ELSE rex.prod_hier_l9
            END AS prod_hier_l9,
            rex.kpi_chnl_wt,
            rex.mkt_share,
            UPPER(rex.ques_desc::TEXT) AS ques_desc,
            rex."y/n_flag",
            rex.rej_reason,
            rex.response,
            rex.response_score,
            rex.acc_rej_reason,
            NULL::CHARACTER VARYING AS actual,
            NULL::CHARACTER VARYING AS "target",
            'Y'::CHARACTER VARYING AS priority_store_flag,
            NULL AS store_grade
        FROM v_rpt_rex_perfect_store rex
            LEFT JOIN (
                SELECT edw_vw_pop6_products.cntry_cd,
                    edw_vw_pop6_products.src_file_date,
                    edw_vw_pop6_products.status,
                    edw_vw_pop6_products.productdb_id,
                    edw_vw_pop6_products.barcode,
                    edw_vw_pop6_products.sku,
                    edw_vw_pop6_products.unit_price,
                    edw_vw_pop6_products.display_order,
                    edw_vw_pop6_products.launch_date,
                    edw_vw_pop6_products.largest_uom_quantity,
                    edw_vw_pop6_products.middle_uom_quantity,
                    edw_vw_pop6_products.smallest_uom_quantity,
                    edw_vw_pop6_products.company,
                    edw_vw_pop6_products.sku_english,
                    edw_vw_pop6_products.sku_code,
                    edw_vw_pop6_products.ps_category,
                    edw_vw_pop6_products.ps_segment,
                    edw_vw_pop6_products.ps_category_segment,
                    edw_vw_pop6_products.country_l1,
                    edw_vw_pop6_products.regional_franchise_l2,
                    edw_vw_pop6_products.franchise_l3,
                    edw_vw_pop6_products.brand_l4,
                    edw_vw_pop6_products.sub_category_l5,
                    edw_vw_pop6_products.platform_l6,
                    edw_vw_pop6_products.variance_l7,
                    edw_vw_pop6_products.pack_size_l8
                FROM edw_vw_pop6_products
            ) prods ON trim (rex.eannumber::TEXT) = trim (prods.barcode::TEXT)
            AND trim (rex.country::TEXT) = trim (prods.country_l1::TEXT)
        WHERE rex.country::TEXT <> 'Hong Kong'::CHARACTER VARYING::TEXT
    ) derived_table1
),
pop6 as (
    SELECT UPPER(v_rpt_pop6_perfectstore.dataset::TEXT)::CHARACTER VARYING AS dataset,
        v_rpt_pop6_perfectstore.merchandisingresponseid,
        v_rpt_pop6_perfectstore.surveyresponseid,
        v_rpt_pop6_perfectstore.customerid,
        v_rpt_pop6_perfectstore.salespersonid,
        v_rpt_pop6_perfectstore.visitid,
        v_rpt_pop6_perfectstore.mrch_resp_startdt::DATE AS mrch_resp_startdt,
        v_rpt_pop6_perfectstore.mrch_resp_enddt::DATE AS mrch_resp_enddt,
        v_rpt_pop6_perfectstore.mrch_resp_status,
        v_rpt_pop6_perfectstore.mastersurveyid,
        v_rpt_pop6_perfectstore.survey_status,
        v_rpt_pop6_perfectstore.survey_enddate::CHARACTER VARYING AS survey_enddate,
        v_rpt_pop6_perfectstore.questionkey,
        v_rpt_pop6_perfectstore.questiontext,
        v_rpt_pop6_perfectstore.valuekey,
        v_rpt_pop6_perfectstore.value,
        v_rpt_pop6_perfectstore.productid,
        UPPER(v_rpt_pop6_perfectstore.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
        NULL AS answerscore,
        UPPER(v_rpt_pop6_perfectstore.presence::TEXT)::CHARACTER VARYING AS presence,
        v_rpt_pop6_perfectstore.outofstock,
        NULL AS mastersurveyname,
        UPPER(v_rpt_pop6_perfectstore.kpi::TEXT)::CHARACTER VARYING AS kpi,
        v_rpt_pop6_perfectstore.category,
        v_rpt_pop6_perfectstore.segment,
        v_rpt_pop6_perfectstore.vst_visitid,
        v_rpt_pop6_perfectstore.scheduleddate,
        v_rpt_pop6_perfectstore.scheduledtime::CHARACTER VARYING AS scheduledtime,
        v_rpt_pop6_perfectstore.duration::CHARACTER VARYING AS duration,
        UPPER(v_rpt_pop6_perfectstore.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
        v_rpt_pop6_perfectstore.fisc_yr::INTEGER AS fisc_yr,
        v_rpt_pop6_perfectstore.fisc_per::INTEGER AS fisc_per,
        v_rpt_pop6_perfectstore.firstname,
        v_rpt_pop6_perfectstore.lastname,
        v_rpt_pop6_perfectstore.cust_remotekey,
        v_rpt_pop6_perfectstore.customername,
        v_rpt_pop6_perfectstore.country,
        v_rpt_pop6_perfectstore.state,
        v_rpt_pop6_perfectstore.county,
        v_rpt_pop6_perfectstore.district,
        v_rpt_pop6_perfectstore.city,
        v_rpt_pop6_perfectstore.storereference,
        v_rpt_pop6_perfectstore.storetype,
        v_rpt_pop6_perfectstore.channel,
        v_rpt_pop6_perfectstore.salesgroup,
        v_rpt_pop6_perfectstore.bu,
        v_rpt_pop6_perfectstore.soldtoparty,
        NULL AS brand,
        v_rpt_pop6_perfectstore.productname,
        v_rpt_pop6_perfectstore.eannumber,
        v_rpt_pop6_perfectstore.matl_num,
        v_rpt_pop6_perfectstore.prod_hier_l1,
        v_rpt_pop6_perfectstore.prod_hier_l2,
        v_rpt_pop6_perfectstore.prod_hier_l3,
        v_rpt_pop6_perfectstore.prod_hier_l4,
        v_rpt_pop6_perfectstore.prod_hier_l5,
        v_rpt_pop6_perfectstore.prod_hier_l6,
        v_rpt_pop6_perfectstore.prod_hier_l7,
        v_rpt_pop6_perfectstore.prod_hier_l8,
        v_rpt_pop6_perfectstore.prod_hier_l9,
        v_rpt_pop6_perfectstore.kpi_chnl_wt,
        v_rpt_pop6_perfectstore.mkt_share,
        UPPER(v_rpt_pop6_perfectstore.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
        v_rpt_pop6_perfectstore."y/n_flag",
        NULL AS posm_execution_flag,
        v_rpt_pop6_perfectstore.rej_reason,
        v_rpt_pop6_perfectstore.response,
        v_rpt_pop6_perfectstore.response_score,
        v_rpt_pop6_perfectstore.acc_rej_reason,
        NULL AS actual,
        NULL AS "target",
        'Y' AS priority_store_flag,
        NULL AS photo_url,
        NULL AS website_url,
        NULL AS store_grade
    FROM v_rpt_pop6_perfectstore
    WHERE UPPER(v_rpt_pop6_perfectstore.country::TEXT) = 'KOREA'::CHARACTER VARYING::TEXT
        OR UPPER(v_rpt_pop6_perfectstore.country::TEXT) = 'TAIWAN'::CHARACTER VARYING::TEXT
        OR UPPER(v_rpt_pop6_perfectstore.country::TEXT) = 'HONG KONG'::CHARACTER VARYING::TEXT
),
pcf as (
    SELECT edw_pacific_perfect_store.dataset,
    edw_pacific_perfect_store.merchandisingresponseid,
    edw_pacific_perfect_store.surveyresponseid,
    edw_pacific_perfect_store.customerid,
    edw_pacific_perfect_store.salespersonid,
    edw_pacific_perfect_store.visitid,
    edw_pacific_perfect_store.mrch_resp_startdt::DATE AS mrch_resp_startdt,
    edw_pacific_perfect_store.mrch_resp_enddt::DATE AS mrch_resp_enddt,
    edw_pacific_perfect_store.mrch_resp_status,
    edw_pacific_perfect_store.mastersurveyid,
    edw_pacific_perfect_store.survey_status,
    edw_pacific_perfect_store.survey_enddate,
    edw_pacific_perfect_store.questionkey,
    edw_pacific_perfect_store.questiontext,
    edw_pacific_perfect_store.valuekey,
    edw_pacific_perfect_store.value::CHARACTER VARYING AS value,
    edw_pacific_perfect_store.productid,
    edw_pacific_perfect_store.mustcarryitem,
    edw_pacific_perfect_store.answerscore,
    edw_pacific_perfect_store.presence,
    edw_pacific_perfect_store.outofstock,
    edw_pacific_perfect_store.mastersurveyname,
    edw_pacific_perfect_store.kpi,
    edw_pacific_perfect_store.category,
    edw_pacific_perfect_store.segment,
    edw_pacific_perfect_store.vst_visitid,
    edw_pacific_perfect_store.scheduleddate,
    edw_pacific_perfect_store.scheduledtime,
    edw_pacific_perfect_store.duration,
    edw_pacific_perfect_store.vst_status,
    edw_pacific_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    edw_pacific_perfect_store.fisc_per::INTEGER AS fisc_per,
    edw_pacific_perfect_store.firstname::CHARACTER VARYING (255) AS firstname,
    edw_pacific_perfect_store.lastname,
    edw_pacific_perfect_store.cust_remotekey,
    edw_pacific_perfect_store.customername,
    edw_pacific_perfect_store.country::CHARACTER VARYING (200) AS country,
    edw_pacific_perfect_store.state,
    edw_pacific_perfect_store.county,
    edw_pacific_perfect_store.district,
    edw_pacific_perfect_store.city,
    edw_pacific_perfect_store.storereference::CHARACTER VARYING (200) AS storereference,
    edw_pacific_perfect_store.storetype,
    edw_pacific_perfect_store.channel,
    edw_pacific_perfect_store.salesgroup::CHARACTER VARYING (200) AS salesgroup,
    NULL AS bu,
    edw_pacific_perfect_store.soldtoparty,
    edw_pacific_perfect_store.brand,
    edw_pacific_perfect_store.productname,
    edw_pacific_perfect_store.eannumber,
    edw_pacific_perfect_store.matl_num,
    edw_pacific_perfect_store.prod_hier_l1,
    edw_pacific_perfect_store.prod_hier_l2,
    edw_pacific_perfect_store.prod_hier_l3,
    edw_pacific_perfect_store.prod_hier_l4,
    edw_pacific_perfect_store.prod_hier_l5,
    edw_pacific_perfect_store.prod_hier_l6,
    edw_pacific_perfect_store.prod_hier_l7,
    edw_pacific_perfect_store.prod_hier_l8,
    edw_pacific_perfect_store.prod_hier_l9,
    edw_pacific_perfect_store.kpi_chnl_wt::DOUBLE precision AS kpi_chnl_wt,
    edw_pacific_perfect_store.mkt_share::DOUBLE precision AS mkt_share,
    edw_pacific_perfect_store.ques_desc,
    edw_pacific_perfect_store."y/n_flag",
    NULL AS posm_execution_flag,
    edw_pacific_perfect_store.rej_reason,
    edw_pacific_perfect_store.response,
    edw_pacific_perfect_store.response_score,
    edw_pacific_perfect_store.acc_rej_reason,
    edw_pacific_perfect_store.actual::CHARACTER VARYING AS actual,
    edw_pacific_perfect_store.target::CHARACTER VARYING AS "target",
    CASE
        WHEN (
            UPPER(
                edw_pacific_perfect_store.firstname::TEXT || edw_pacific_perfect_store.lastname::TEXT
            )::CHARACTER VARYING (300) IN (
                SELECT DISTINCT itg_query_parameters.parameter_value
                FROM itg_query_parameters
                WHERE itg_query_parameters.country_code::TEXT = 'AU'::CHARACTER VARYING::TEXT
                    AND itg_query_parameters.parameter_name::TEXT = 'NON_PRIORITY_STORES'::CHARACTER VARYING::TEXT
            )
        ) THEN 'N'::CHARACTER VARYING
        ELSE 'Y'::CHARACTER VARYING
    END AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    store_grade
FROM edw_pacific_perfect_store
),
phl as (
    SELECT UPPER(v_rpt_ph_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    v_rpt_ph_perfect_store.customerid,
    v_rpt_ph_perfect_store.salespersonid,
    NULL AS visitid,
    v_rpt_ph_perfect_store.mrch_resp_startdt,
    v_rpt_ph_perfect_store.mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    v_rpt_ph_perfect_store.survey_enddate::CHARACTER VARYING AS survey_enddate,
    NULL AS questionkey,
    v_rpt_ph_perfect_store.questiontext,
    NULL AS valuekey,
    v_rpt_ph_perfect_store.value,
    NULL AS productid,
    UPPER(v_rpt_ph_perfect_store.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    v_rpt_ph_perfect_store.answerscore,
    UPPER(v_rpt_ph_perfect_store.presence::TEXT)::CHARACTER VARYING AS presence,
    v_rpt_ph_perfect_store.outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_ph_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_ph_perfect_store.category,
    v_rpt_ph_perfect_store.segment,
    NULL AS vst_visitid,
    v_rpt_ph_perfect_store.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(v_rpt_ph_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_ph_perfect_store.fisc_yr,
    v_rpt_ph_perfect_store.fisc_per::INTEGER AS fisc_per,
    v_rpt_ph_perfect_store.firstname,
    v_rpt_ph_perfect_store.lastname,
    NULL AS cust_remotekey,
    v_rpt_ph_perfect_store.customername,
    v_rpt_ph_perfect_store.country,
    NULL AS state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    v_rpt_ph_perfect_store.storereference,
    v_rpt_ph_perfect_store.storetype,
    v_rpt_ph_perfect_store.channel,
    v_rpt_ph_perfect_store.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    NULL AS brand,
    NULL AS productname,
    NULL AS eannumber,
    NULL AS matl_num,
    NULL AS prod_hier_l1,
    NULL AS prod_hier_l2,
    v_rpt_ph_perfect_store.prod_hier_l3,
    v_rpt_ph_perfect_store.prod_hier_l4,
    v_rpt_ph_perfect_store.prod_hier_l5,
    v_rpt_ph_perfect_store.prod_hier_l6,
    NULL AS prod_hier_l7,
    NULL AS prod_hier_l8,
    NULL AS prod_hier_l9,
    v_rpt_ph_perfect_store.kpi_chnl_wt,
    v_rpt_ph_perfect_store.mkt_share,
    UPPER(v_rpt_ph_perfect_store.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    v_rpt_ph_perfect_store."y/n_flag",
    NULL AS posm_execution_flag,
    '' AS rej_reason,
    NULL AS response,
    NULL AS response_score,
    '' AS acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_ph_perfect_store
),
idn as (
    SELECT UPPER(v_rpt_id_regional_perfect_store.dataset)::CHARACTER VARYING AS dataset,
    v_rpt_id_regional_perfect_store.merchandisingresponseid::CHARACTER VARYING AS merchandisingresponseid,
    v_rpt_id_regional_perfect_store.surveyresponseid::CHARACTER VARYING AS surveyresponseid,
    v_rpt_id_regional_perfect_store.customerid,
    v_rpt_id_regional_perfect_store.salespersonid,
    v_rpt_id_regional_perfect_store.visitid::CHARACTER VARYING AS visitid,
    v_rpt_id_regional_perfect_store.mrch_resp_startdt,
    v_rpt_id_regional_perfect_store.mrch_resp_enddt,
    v_rpt_id_regional_perfect_store.mrch_resp_status::CHARACTER VARYING AS mrch_resp_status,
    v_rpt_id_regional_perfect_store.mastersurveyid::CHARACTER VARYING AS mastersurveyid,
    v_rpt_id_regional_perfect_store.survey_status::CHARACTER VARYING AS survey_status,
    v_rpt_id_regional_perfect_store.survey_enddate,
    v_rpt_id_regional_perfect_store.questionkey::CHARACTER VARYING AS questionkey,
    v_rpt_id_regional_perfect_store.questiontext,
    v_rpt_id_regional_perfect_store.valuekey::CHARACTER VARYING AS valuekey,
    v_rpt_id_regional_perfect_store.value,
    v_rpt_id_regional_perfect_store.productid::CHARACTER VARYING AS productid,
    UPPER(v_rpt_id_regional_perfect_store.mustcarryitem)::CHARACTER VARYING AS mustcarryitem,
    v_rpt_id_regional_perfect_store.answerscore,
    UPPER(v_rpt_id_regional_perfect_store.presence)::CHARACTER VARYING AS presence,
    v_rpt_id_regional_perfect_store.outofstock,
    v_rpt_id_regional_perfect_store.mastersurveyname::CHARACTER VARYING AS mastersurveyname,
    UPPER(v_rpt_id_regional_perfect_store.kpi)::CHARACTER VARYING AS kpi,
    v_rpt_id_regional_perfect_store.category,
    v_rpt_id_regional_perfect_store.segment,
    v_rpt_id_regional_perfect_store.vst_visitid::CHARACTER VARYING AS vst_visitid,
    v_rpt_id_regional_perfect_store.scheduleddate,
    v_rpt_id_regional_perfect_store.scheduledtime::CHARACTER VARYING AS scheduledtime,
    v_rpt_id_regional_perfect_store.duration::CHARACTER VARYING AS duration,
    UPPER(v_rpt_id_regional_perfect_store.vst_status)::CHARACTER VARYING AS vst_status,
    v_rpt_id_regional_perfect_store.fisc_yr,
    v_rpt_id_regional_perfect_store.fisc_per,
    v_rpt_id_regional_perfect_store.firstname,
    v_rpt_id_regional_perfect_store.lastname,
    v_rpt_id_regional_perfect_store.cust_remotekey::CHARACTER VARYING AS cust_remotekey,
    v_rpt_id_regional_perfect_store.customername,
    v_rpt_id_regional_perfect_store.country,
    v_rpt_id_regional_perfect_store.state,
    v_rpt_id_regional_perfect_store.county::CHARACTER VARYING AS county,
    v_rpt_id_regional_perfect_store.district::CHARACTER VARYING AS district,
    v_rpt_id_regional_perfect_store.city,
    UPPER(
        v_rpt_id_regional_perfect_store.storereference::TEXT
    )::CHARACTER VARYING AS storereference,
    v_rpt_id_regional_perfect_store.storetype,
    v_rpt_id_regional_perfect_store.channel,
    v_rpt_id_regional_perfect_store.salesgroup,
    NULL AS bu,
    v_rpt_id_regional_perfect_store.soldtoparty::CHARACTER VARYING AS soldtoparty,
    v_rpt_id_regional_perfect_store.brand::CHARACTER VARYING AS brand,
    v_rpt_id_regional_perfect_store.productname::CHARACTER VARYING AS productname,
    v_rpt_id_regional_perfect_store.eannumber::CHARACTER VARYING AS eannumber,
    v_rpt_id_regional_perfect_store.matl_num::CHARACTER VARYING AS matl_num,
    v_rpt_id_regional_perfect_store.prod_hier_l1::CHARACTER VARYING AS prod_hier_l1,
    v_rpt_id_regional_perfect_store.prod_hier_l2::CHARACTER VARYING AS prod_hier_l2,
    v_rpt_id_regional_perfect_store.prod_hier_l3,
    v_rpt_id_regional_perfect_store.prod_hier_l4,
    v_rpt_id_regional_perfect_store.prod_hier_l5,
    v_rpt_id_regional_perfect_store.prod_hier_l6,
    v_rpt_id_regional_perfect_store.prod_hier_l7::CHARACTER VARYING AS prod_hier_l7,
    v_rpt_id_regional_perfect_store.prod_hier_l8::CHARACTER VARYING AS prod_hier_l8,
    v_rpt_id_regional_perfect_store.prod_hier_l9::CHARACTER VARYING AS prod_hier_l9,
    v_rpt_id_regional_perfect_store.kpi_chnl_wt,
    v_rpt_id_regional_perfect_store.mkt_share,
    UPPER(v_rpt_id_regional_perfect_store.ques_desc)::CHARACTER VARYING AS ques_desc,
    v_rpt_id_regional_perfect_store."y/n_flag",
    v_rpt_id_regional_perfect_store.posm_execution_flag,
    v_rpt_id_regional_perfect_store.rej_reason,
    v_rpt_id_regional_perfect_store.response::CHARACTER VARYING AS response,
    v_rpt_id_regional_perfect_store.response_score::CHARACTER VARYING AS response_score,
    v_rpt_id_regional_perfect_store.acc_rej_reason::CHARACTER VARYING AS acc_rej_reason,
    v_rpt_id_regional_perfect_store.actual::CHARACTER VARYING AS actual,
    v_rpt_id_regional_perfect_store.target::CHARACTER VARYING AS "target",
    v_rpt_id_regional_perfect_store.priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_id_regional_perfect_store
),
jpn as (
    SELECT UPPER(v_rpt_jp_pop6_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    v_rpt_jp_pop6_perfect_store.merchandisingresponseid,
    v_rpt_jp_pop6_perfect_store.surveyresponseid,
    v_rpt_jp_pop6_perfect_store.customerid,
    v_rpt_jp_pop6_perfect_store.salespersonid,
    v_rpt_jp_pop6_perfect_store.visitid,
    v_rpt_jp_pop6_perfect_store.mrch_resp_startdt::DATE AS mrch_resp_startdt,
    v_rpt_jp_pop6_perfect_store.mrch_resp_enddt::DATE AS mrch_resp_enddt,
    v_rpt_jp_pop6_perfect_store.mrch_resp_status,
    v_rpt_jp_pop6_perfect_store.mastersurveyid,
    v_rpt_jp_pop6_perfect_store.survey_status,
    v_rpt_jp_pop6_perfect_store.survey_enddate::CHARACTER VARYING AS survey_enddate,
    v_rpt_jp_pop6_perfect_store.questionkey,
    v_rpt_jp_pop6_perfect_store.questiontext,
    v_rpt_jp_pop6_perfect_store.valuekey,
    v_rpt_jp_pop6_perfect_store.value,
    v_rpt_jp_pop6_perfect_store.productid,
    UPPER(v_rpt_jp_pop6_perfect_store.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(v_rpt_jp_pop6_perfect_store.presence::TEXT)::CHARACTER VARYING AS presence,
    v_rpt_jp_pop6_perfect_store.outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_jp_pop6_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_jp_pop6_perfect_store.category,
    v_rpt_jp_pop6_perfect_store.segment,
    v_rpt_jp_pop6_perfect_store.vst_visitid,
    v_rpt_jp_pop6_perfect_store.scheduleddate,
    v_rpt_jp_pop6_perfect_store.scheduledtime::CHARACTER VARYING AS scheduledtime,
    v_rpt_jp_pop6_perfect_store.duration::CHARACTER VARYING AS duration,
    UPPER(v_rpt_jp_pop6_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_jp_pop6_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    v_rpt_jp_pop6_perfect_store.fisc_per::INTEGER AS fisc_per,
    v_rpt_jp_pop6_perfect_store.firstname,
    v_rpt_jp_pop6_perfect_store.lastname,
    v_rpt_jp_pop6_perfect_store.cust_remotekey,
    v_rpt_jp_pop6_perfect_store.customername,
    v_rpt_jp_pop6_perfect_store.country,
    v_rpt_jp_pop6_perfect_store.state,
    v_rpt_jp_pop6_perfect_store.county,
    v_rpt_jp_pop6_perfect_store.district,
    v_rpt_jp_pop6_perfect_store.city,
    v_rpt_jp_pop6_perfect_store.storereference,
    v_rpt_jp_pop6_perfect_store.storetype,
    v_rpt_jp_pop6_perfect_store.channel,
    v_rpt_jp_pop6_perfect_store.salesgroup,
    v_rpt_jp_pop6_perfect_store.bu,
    v_rpt_jp_pop6_perfect_store.soldtoparty,
    NULL AS brand,
    v_rpt_jp_pop6_perfect_store.productname,
    v_rpt_jp_pop6_perfect_store.eannumber,
    v_rpt_jp_pop6_perfect_store.matl_num,
    v_rpt_jp_pop6_perfect_store.prod_hier_l1,
    v_rpt_jp_pop6_perfect_store.prod_hier_l2,
    v_rpt_jp_pop6_perfect_store.prod_hier_l3,
    v_rpt_jp_pop6_perfect_store.prod_hier_l4,
    v_rpt_jp_pop6_perfect_store.prod_hier_l5,
    v_rpt_jp_pop6_perfect_store.prod_hier_l6,
    v_rpt_jp_pop6_perfect_store.prod_hier_l7,
    v_rpt_jp_pop6_perfect_store.prod_hier_l8,
    v_rpt_jp_pop6_perfect_store.prod_hier_l9,
    v_rpt_jp_pop6_perfect_store.kpi_chnl_wt,
    v_rpt_jp_pop6_perfect_store.mkt_share,
    UPPER(v_rpt_jp_pop6_perfect_store.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    v_rpt_jp_pop6_perfect_store."y/n_flag",
    NULL AS posm_execution_flag,
    v_rpt_jp_pop6_perfect_store.rej_reason,
    v_rpt_jp_pop6_perfect_store.response,
    v_rpt_jp_pop6_perfect_store.response_score,
    v_rpt_jp_pop6_perfect_store.acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_jp_pop6_perfect_store
),
sgp as (
    SELECT UPPER(v_rpt_sg_pop6_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    v_rpt_sg_pop6_perfect_store.merchandisingresponseid,
    v_rpt_sg_pop6_perfect_store.surveyresponseid,
    v_rpt_sg_pop6_perfect_store.customerid,
    v_rpt_sg_pop6_perfect_store.salespersonid,
    v_rpt_sg_pop6_perfect_store.visitid,
    v_rpt_sg_pop6_perfect_store.mrch_resp_startdt::DATE AS mrch_resp_startdt,
    v_rpt_sg_pop6_perfect_store.mrch_resp_enddt::DATE AS mrch_resp_enddt,
    v_rpt_sg_pop6_perfect_store.mrch_resp_status,
    v_rpt_sg_pop6_perfect_store.mastersurveyid,
    v_rpt_sg_pop6_perfect_store.survey_status,
    v_rpt_sg_pop6_perfect_store.survey_enddate::CHARACTER VARYING AS survey_enddate,
    v_rpt_sg_pop6_perfect_store.questionkey,
    v_rpt_sg_pop6_perfect_store.questiontext,
    v_rpt_sg_pop6_perfect_store.valuekey,
    v_rpt_sg_pop6_perfect_store.value,
    v_rpt_sg_pop6_perfect_store.productid,
    UPPER(v_rpt_sg_pop6_perfect_store.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(v_rpt_sg_pop6_perfect_store.presence::TEXT)::CHARACTER VARYING AS presence,
    v_rpt_sg_pop6_perfect_store.outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_sg_pop6_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_sg_pop6_perfect_store.category,
    v_rpt_sg_pop6_perfect_store.segment,
    v_rpt_sg_pop6_perfect_store.vst_visitid,
    v_rpt_sg_pop6_perfect_store.scheduleddate,
    v_rpt_sg_pop6_perfect_store.scheduledtime::CHARACTER VARYING AS scheduledtime,
    v_rpt_sg_pop6_perfect_store.duration::CHARACTER VARYING AS duration,
    UPPER(v_rpt_sg_pop6_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_sg_pop6_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    v_rpt_sg_pop6_perfect_store.fisc_per::INTEGER AS fisc_per,
    v_rpt_sg_pop6_perfect_store.firstname,
    v_rpt_sg_pop6_perfect_store.lastname,
    v_rpt_sg_pop6_perfect_store.cust_remotekey,
    v_rpt_sg_pop6_perfect_store.customername,
    v_rpt_sg_pop6_perfect_store.country,
    v_rpt_sg_pop6_perfect_store.state,
    v_rpt_sg_pop6_perfect_store.county,
    v_rpt_sg_pop6_perfect_store.district,
    v_rpt_sg_pop6_perfect_store.city,
    v_rpt_sg_pop6_perfect_store.storereference,
    v_rpt_sg_pop6_perfect_store.storetype,
    v_rpt_sg_pop6_perfect_store.channel,
    v_rpt_sg_pop6_perfect_store.salesgroup,
    v_rpt_sg_pop6_perfect_store.bu,
    v_rpt_sg_pop6_perfect_store.soldtoparty,
    NULL AS brand,
    v_rpt_sg_pop6_perfect_store.productname,
    v_rpt_sg_pop6_perfect_store.eannumber,
    v_rpt_sg_pop6_perfect_store.matl_num,
    v_rpt_sg_pop6_perfect_store.prod_hier_l1,
    v_rpt_sg_pop6_perfect_store.prod_hier_l2,
    v_rpt_sg_pop6_perfect_store.prod_hier_l3,
    v_rpt_sg_pop6_perfect_store.prod_hier_l4,
    v_rpt_sg_pop6_perfect_store.prod_hier_l5,
    v_rpt_sg_pop6_perfect_store.prod_hier_l6,
    v_rpt_sg_pop6_perfect_store.prod_hier_l7,
    v_rpt_sg_pop6_perfect_store.prod_hier_l8,
    v_rpt_sg_pop6_perfect_store.prod_hier_l9,
    v_rpt_sg_pop6_perfect_store.kpi_chnl_wt,
    v_rpt_sg_pop6_perfect_store.mkt_share,
    UPPER(v_rpt_sg_pop6_perfect_store.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    v_rpt_sg_pop6_perfect_store."y/n_flag",
    NULL AS posm_execution_flag,
    v_rpt_sg_pop6_perfect_store.rej_reason,
    v_rpt_sg_pop6_perfect_store.response,
    v_rpt_sg_pop6_perfect_store.response_score,
    v_rpt_sg_pop6_perfect_store.acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_sg_pop6_perfect_store
),
tha_1 as (
    SELECT UPPER(dataset) AS dataset,
    merchandisingresponseid,
    surveyresponseid,
    customerid,
    salespersonid,
    visitid,
    mrch_resp_startdt AS mrch_resp_startdt,
    mrch_resp_enddt AS mrch_resp_enddt,
    mrch_resp_status,
    mastersurveyid,
    survey_status,
    cast(survey_enddate as VARCHAR) AS survey_enddate,
    questionkey,
    questiontext,
    valuekey,
    value,
    productid,
    UPPER(mustcarryitem) AS mustcarryitem,
    NULL AS answerscore,
    UPPER(presence) AS presence,
    outofstock,
    NULL AS mastersurveyname,
    UPPER(kpi) AS kpi,
    category,
    segment,
    vst_visitid,
    scheduleddate,
    cast(scheduledtime as varchar) AS scheduledtime,
    cast(duration as varchar) AS duration,
    UPPER(vst_status) AS vst_status,
    cast(fisc_yr as INTEGER) AS fisc_yr,
    cast(fisc_per as INTEGER) AS fisc_per,
    firstname,
    lastname,
    cust_remotekey,
    customername,
    country,
    state,
    county,
    district,
    city,
    storereference,
    upper(storetype) as storetype,
    upper(channel) as channel,
    salesgroup,
    bu,
    soldtoparty,
    NULL AS brand,
    productname,
    eannumber,
    matl_num,
    prod_hier_l1,
    prod_hier_l2,
    prod_hier_l3,
    prod_hier_l4,
    prod_hier_l5,
    prod_hier_l6,
    prod_hier_l7,
    prod_hier_l8,
    prod_hier_l9,
    kpi_chnl_wt,
    mkt_share,
    UPPER(ques_desc) AS ques_desc,
    "y/n_flag",
    NULL AS posm_execution_flag,
    rej_reason,
    response,
    response_score,
    acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_th_pop6_perfect_store
),
tha_2 as (
SELECT UPPER(edw_rpt_th_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    edw_rpt_th_perfect_store.customerid,
    edw_rpt_th_perfect_store.salespersonid,
    NULL AS visitid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    NULL AS survey_enddate,
    NULL AS questionkey,
    edw_rpt_th_perfect_store.questiontext,
    NULL AS valuekey,
    edw_rpt_th_perfect_store.value::CHARACTER VARYING AS value,
    NULL AS productid,
    UPPER(edw_rpt_th_perfect_store.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    UPPER(edw_rpt_th_perfect_store.answerscore::TEXT)::CHARACTER VARYING AS answerscore,
    UPPER(edw_rpt_th_perfect_store.presence::TEXT)::CHARACTER VARYING AS presence,
    edw_rpt_th_perfect_store.outofstock,
    NULL AS mastersurveyname,
    UPPER(edw_rpt_th_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    edw_rpt_th_perfect_store.category AS category,
    edw_rpt_th_perfect_store.brand AS segment,
    NULL AS vst_visitid,
    edw_rpt_th_perfect_store.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(edw_rpt_th_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    edw_rpt_th_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    edw_rpt_th_perfect_store.fisc_per::INTEGER AS fisc_per,
    edw_rpt_th_perfect_store.firstname,
    edw_rpt_th_perfect_store.lastname,
    NULL AS cust_remotekey,
    edw_rpt_th_perfect_store.customername,
    edw_rpt_th_perfect_store.country,
    edw_rpt_th_perfect_store.state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    edw_rpt_th_perfect_store.storereference,
    edw_rpt_th_perfect_store.storetype,
    edw_rpt_th_perfect_store.channel,
    edw_rpt_th_perfect_store.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    NULL AS brand,
    edw_rpt_th_perfect_store.prod_hier_l9 AS productname,
    edw_rpt_th_perfect_store.eannumber AS eannumber,
    NULL AS matl_num,
    edw_rpt_th_perfect_store.prod_hier_l1,
    edw_rpt_th_perfect_store.prod_hier_l2,
    edw_rpt_th_perfect_store.prod_hier_l3,
    edw_rpt_th_perfect_store.prod_hier_l4,
    edw_rpt_th_perfect_store.prod_hier_l5,
    edw_rpt_th_perfect_store.prod_hier_l6,
    edw_rpt_th_perfect_store.prod_hier_l7,
    edw_rpt_th_perfect_store.prod_hier_l8,
    edw_rpt_th_perfect_store.prod_hier_l9,
    edw_rpt_th_perfect_store.kpi_chnl_wt,
    edw_rpt_th_perfect_store.mkt_share,
    UPPER(edw_rpt_th_perfect_store.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    edw_rpt_th_perfect_store."y/n_flag",
    edw_rpt_th_perfect_store.posm_execution_flag,
    edw_rpt_th_perfect_store.rej_reason,
    NULL AS response,
    NULL AS response_score,
    NULL AS acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    UPPER(
        edw_rpt_th_perfect_store.priority_store_flag::TEXT
    )::CHARACTER VARYING AS priority_store_flag,
    UPPER(edw_rpt_th_perfect_store.photo_url::TEXT)::CHARACTER VARYING AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM edw_rpt_th_perfect_store
),
mys_1 as (
    SELECT UPPER(v_rpt_my_perfect_store_snapshot.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    v_rpt_my_perfect_store_snapshot.customerid,
    v_rpt_my_perfect_store_snapshot.salespersonid,
    NULL AS visitid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    NULL AS survey_enddate,
    NULL AS questionkey,
    v_rpt_my_perfect_store_snapshot.questiontext,
    NULL AS valuekey,
    v_rpt_my_perfect_store_snapshot.value::CHARACTER VARYING AS value,
    NULL AS productid,
    UPPER(
        v_rpt_my_perfect_store_snapshot.mustcarryitem::TEXT
    )::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(v_rpt_my_perfect_store_snapshot.presence::TEXT)::CHARACTER VARYING AS presence,
    v_rpt_my_perfect_store_snapshot.outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_my_perfect_store_snapshot.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_my_perfect_store_snapshot.prod_hier_l5 AS category,
    NULL AS segment,
    NULL AS vst_visitid,
    v_rpt_my_perfect_store_snapshot.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(v_rpt_my_perfect_store_snapshot.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_my_perfect_store_snapshot.fisc_yr::INTEGER AS fisc_yr,
    v_rpt_my_perfect_store_snapshot.fisc_per,
    v_rpt_my_perfect_store_snapshot.firstname,
    v_rpt_my_perfect_store_snapshot.lastname,
    NULL AS cust_remotekey,
    v_rpt_my_perfect_store_snapshot.customername,
    v_rpt_my_perfect_store_snapshot.country,
    v_rpt_my_perfect_store_snapshot.state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    v_rpt_my_perfect_store_snapshot.storereference,
    v_rpt_my_perfect_store_snapshot.storetype,
    v_rpt_my_perfect_store_snapshot.channel,
    v_rpt_my_perfect_store_snapshot.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    NULL AS brand,
    v_rpt_my_perfect_store_snapshot.prod_hier_l9 AS productname,
    NULL AS eannumber,
    NULL AS matl_num,
    v_rpt_my_perfect_store_snapshot.prod_hier_l1,
    v_rpt_my_perfect_store_snapshot.prod_hier_l2,
    v_rpt_my_perfect_store_snapshot.prod_hier_l3,
    v_rpt_my_perfect_store_snapshot.prod_hier_l4,
    v_rpt_my_perfect_store_snapshot.prod_hier_l5,
    v_rpt_my_perfect_store_snapshot.prod_hier_l6,
    v_rpt_my_perfect_store_snapshot.prod_hier_l7,
    v_rpt_my_perfect_store_snapshot.prod_hier_l8,
    v_rpt_my_perfect_store_snapshot.prod_hier_l9,
    v_rpt_my_perfect_store_snapshot.kpi_chnl_wt,
    v_rpt_my_perfect_store_snapshot.mkt_share,
    UPPER(v_rpt_my_perfect_store_snapshot.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    v_rpt_my_perfect_store_snapshot."y/n_flag",
    v_rpt_my_perfect_store_snapshot.posm_execution_flag,
    v_rpt_my_perfect_store_snapshot.rej_reason,
    NULL AS response,
    NULL AS response_score,
    NULL AS acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_my_perfect_store_snapshot
where fisc_per < '202307'
),
mys_2 as (
    SELECT UPPER(v_rpt_my_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    v_rpt_my_perfect_store.customerid,
    v_rpt_my_perfect_store.salespersonid,
    NULL AS visitid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    NULL AS survey_enddate,
    NULL AS questionkey,
    v_rpt_my_perfect_store.questiontext,
    NULL AS valuekey,
    v_rpt_my_perfect_store.value::CHARACTER VARYING AS value,
    NULL AS productid,
    UPPER(v_rpt_my_perfect_store.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(v_rpt_my_perfect_store.presence::TEXT)::CHARACTER VARYING AS presence,
    v_rpt_my_perfect_store.outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_my_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_my_perfect_store.prod_hier_l5 AS category,
    NULL AS segment,
    NULL AS vst_visitid,
    v_rpt_my_perfect_store.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(v_rpt_my_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_my_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    v_rpt_my_perfect_store.fisc_per,
    v_rpt_my_perfect_store.firstname,
    v_rpt_my_perfect_store.lastname,
    NULL AS cust_remotekey,
    v_rpt_my_perfect_store.customername,
    v_rpt_my_perfect_store.country,
    v_rpt_my_perfect_store.state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    v_rpt_my_perfect_store.storereference,
    v_rpt_my_perfect_store.storetype,
    v_rpt_my_perfect_store.channel,
    v_rpt_my_perfect_store.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    NULL AS brand,
    v_rpt_my_perfect_store.prod_hier_l9 AS productname,
    NULL AS eannumber,
    NULL AS matl_num,
    v_rpt_my_perfect_store.prod_hier_l1,
    v_rpt_my_perfect_store.prod_hier_l2,
    v_rpt_my_perfect_store.prod_hier_l3,
    v_rpt_my_perfect_store.prod_hier_l4,
    v_rpt_my_perfect_store.prod_hier_l5,
    v_rpt_my_perfect_store.prod_hier_l6,
    v_rpt_my_perfect_store.prod_hier_l7,
    v_rpt_my_perfect_store.prod_hier_l8,
    v_rpt_my_perfect_store.prod_hier_l9,
    v_rpt_my_perfect_store.kpi_chnl_wt,
    v_rpt_my_perfect_store.mkt_share,
    UPPER(v_rpt_my_perfect_store.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    v_rpt_my_perfect_store."y/n_flag",
    v_rpt_my_perfect_store.posm_execution_flag,
    v_rpt_my_perfect_store.rej_reason,
    NULL AS response,
    NULL AS response_score,
    NULL AS acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    'Y' AS priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_my_perfect_store
where fisc_per >= '202307'
),
ind_1 as (
    SELECT UPPER(v_rpt_in_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    v_rpt_in_perfect_store.customerid,
    v_rpt_in_perfect_store.salespersonid,
    NULL AS visitid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    NULL AS survey_enddate,
    NULL AS questionkey,
    v_rpt_in_perfect_store.questiontext,
    NULL AS valuekey,
    v_rpt_in_perfect_store.value::CHARACTER VARYING AS value,
    NULL AS productid,
    UPPER(v_rpt_in_perfect_store.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(v_rpt_in_perfect_store.presence::TEXT)::CHARACTER VARYING AS presence,
    v_rpt_in_perfect_store.outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_in_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_in_perfect_store.prod_hier_l3 AS category,
    v_rpt_in_perfect_store.prod_hier_l4 AS segment,
    NULL AS vst_visitid,
    v_rpt_in_perfect_store.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(v_rpt_in_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_in_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    v_rpt_in_perfect_store.fisc_per,
    v_rpt_in_perfect_store.firstname,
    v_rpt_in_perfect_store.lastname,
    NULL AS cust_remotekey,
    v_rpt_in_perfect_store.customername,
    v_rpt_in_perfect_store.country,
    v_rpt_in_perfect_store.state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    UPPER(v_rpt_in_perfect_store.storereference::TEXT)::CHARACTER VARYING (255) AS storereference,
    v_rpt_in_perfect_store.storetype,
    v_rpt_in_perfect_store.channel,
    v_rpt_in_perfect_store.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    v_rpt_in_perfect_store.prod_hier_l4 AS brand,
    v_rpt_in_perfect_store.prod_hier_l9 AS productname,
    NULL AS eannumber,
    NULL AS matl_num,
    v_rpt_in_perfect_store.prod_hier_l1,
    v_rpt_in_perfect_store.prod_hier_l2,
    v_rpt_in_perfect_store.prod_hier_l3,
    v_rpt_in_perfect_store.prod_hier_l4,
    v_rpt_in_perfect_store.prod_hier_l5,
    v_rpt_in_perfect_store.prod_hier_l6,
    v_rpt_in_perfect_store.prod_hier_l7,
    v_rpt_in_perfect_store.prod_hier_l8,
    v_rpt_in_perfect_store.prod_hier_l9,
    v_rpt_in_perfect_store.kpi_chnl_wt,
    v_rpt_in_perfect_store.mkt_share,
    v_rpt_in_perfect_store.ques_desc,
    v_rpt_in_perfect_store."y/n_flag",
    NULL AS posm_execution_flag,
    v_rpt_in_perfect_store.rej_reason,
    NULL AS response,
    NULL AS response_score,
    NULL AS acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    v_rpt_in_perfect_store.priority_store_flag,
    v_rpt_in_perfect_store.photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM (
        (
            (
                (
                    (
                        (
                            SELECT 'Merchandising_Response' AS dataset,
                                sku_recom.retailer_cd AS customerid,
                                sku_recom.salesman_code AS salespersonid,
                                'true' AS mustcarryitem,
                                NULL AS answerscore,
                                CASE
                                    WHEN sku_recom.msl_hit = 1 THEN 'true'::CHARACTER VARYING
                                    ELSE 'false'::CHARACTER VARYING
                                END AS presence,
                                NULL AS outofstock,
                                'MSL Compliance' AS kpi,
                                TO_DATE(
                                    concat(
                                        substring (
                                            sku_recom.year_month::CHARACTER VARYING::TEXT,
                                            0,
                                            4
                                        ),
                                        '-'::CHARACTER VARYING::TEXT,
                                        substring (
                                            sku_recom.year_month::CHARACTER VARYING::TEXT,
                                            5,
                                            7
                                        ),
                                        '-15'::CHARACTER VARYING::TEXT
                                    ),
                                    'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                                ) AS scheduleddate,
                                'completed' AS vst_status,
                                substring(
                                    sku_recom.year_month::CHARACTER VARYING::TEXT,
                                    0,
                                    4
                                )::CHARACTER VARYING AS fisc_yr,
                                sku_recom.year_month AS fisc_per,
                                sku_recom.salesman_name AS firstname,
                                '' AS lastname,
                                (
                                    concat(
                                        COALESCE(sku_recom.retailer_cd, 'NA'::CHARACTER VARYING)::TEXT,
                                        '-'::CHARACTER VARYING::TEXT,
                                        COALESCE(
                                            sku_recom.retailer_name,
                                            'NA'::CHARACTER VARYING::TEXT
                                        )
                                    )
                                )::CHARACTER VARYING AS customername,
                                'India' AS country,
                                sku_recom.region_name AS state,
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
                                wt.weight::DOUBLE precision AS kpi_chnl_wt,
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
                                            WHEN derived_table2.msl_target IS NOT NULL THEN derived_table2.msl_hit
                                            ELSE NULL::INTEGER
                                        END AS msl_hit
                                    FROM (
                                            SELECT a.year_month,
                                                a.channel_name,
                                                a.customer_code,
                                                a.customer_name,
                                                a.retailer_category_name,
                                                CASE
                                                    WHEN a.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT THEN concat(
                                                        c.columnname,
                                                        ' ('::CHARACTER VARYING::TEXT,
                                                        c.program_name,
                                                        ')'
                                                    )::CHARACTER VARYING::TEXT
                                                    ELSE c.columnname
                                                END AS program_name,
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
                                                    WHEN SUM(a.msl_target::NUMERIC::NUMERIC(18, 0)) > 0::NUMERIC::NUMERIC(18, 0) THEN 1
                                                    ELSE NULL::INTEGER
                                                END AS msl_target,
                                                CASE
                                                    WHEN SUM(b.msl_hit::NUMERIC::NUMERIC(18, 0)) > 0::NUMERIC::NUMERIC(18, 0) THEN 1
                                                    ELSE 0
                                                END AS msl_hit
                                            FROM (
                                                    SELECT edw_sku_recom.mth_mm AS year_month,
                                                        edw_sku_recom.qtr,
                                                        TO_DATE(
                                                            concat(edw_sku_recom.mth_mm, 15)::CHARACTER VARYING::TEXT,
                                                            'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                        ) AS DATE,
                                                        edw_sku_recom.channel_name,
                                                        edw_sku_recom.retailer_category_name,
                                                        edw_sku_recom.region_name,
                                                        edw_sku_recom.salesman_code,
                                                        edw_sku_recom.salesman_name,
                                                        edw_sku_recom.cust_cd AS customer_code,
                                                        "max"(edw_sku_recom.customer_name::TEXT) AS customer_name,
                                                        edw_sku_recom.class_desc AS retailer_class,
                                                        edw_sku_recom.retailer_cd,
                                                        edw_sku_recom.rtruniquecode AS urc,
                                                        "max"(edw_sku_recom.retailer_name::TEXT) AS retailer_name,
                                                        edw_sku_recom.franchise_name,
                                                        edw_sku_recom.brand_name,
                                                        edw_sku_recom.product_category_name,
                                                        edw_sku_recom.variant_name,
                                                        edw_sku_recom.mothersku_name,
                                                        edw_sku_recom.ms_flag AS msl_target,
                                                        edw_sku_recom.hit_ms_flag AS msl_hit
                                                    FROM edw_sku_recom
                                                    WHERE edw_sku_recom.mothersku_name IS NOT NULL
                                                        AND (
                                                            edw_sku_recom.channel_name::TEXT = 'GT'::CHARACTER VARYING::TEXT
                                                            OR edw_sku_recom.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT
                                                        )
                                                        AND substring(
                                                            edw_sku_recom.mth_mm::CHARACTER VARYING::TEXT,
                                                            0,
                                                            4
                                                        ) >= (
                                                            date_part(
                                                                year,
                                                                convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                                                            ) - 1
                                                        )::CHARACTER VARYING::TEXT
                                                        AND edw_sku_recom.ms_flag::TEXT = 1::CHARACTER VARYING::TEXT
                                                    GROUP BY edw_sku_recom.mth_mm,
                                                        edw_sku_recom.qtr,
                                                        TO_DATE(
                                                            concat(edw_sku_recom.mth_mm, 15)::CHARACTER VARYING::TEXT,
                                                            'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                        ),
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
                                                JOIN (
                                                    SELECT derived_table1."year",
                                                        derived_table1.quarter,
                                                        derived_table1."month",
                                                        derived_table1.distcode,
                                                        derived_table1.retailer_code,
                                                        MIN(derived_table1.columnname::TEXT) AS columnname,
                                                        "max"(derived_table1.program_name::TEXT) AS program_name
                                                    FROM (
                                                            SELECT t.cal_yr AS "year",
                                                                t.cal_qtr_1 AS quarter,
                                                                t.cal_mo_2 AS "month",
                                                                u.columnname,
                                                                u.columnvalue AS program_name,
                                                                u.mastervaluecode AS retailer_code,
                                                                u.distcode
                                                            FROM itg_udcdetails u
                                                                JOIN edw_calendar_dim t ON "right" (u.columnname::TEXT, 4) = t.cal_yr::CHARACTER VARYING::TEXT
                                                                AND "left" (
                                                                    SPLIT_PART (
                                                                        u.columnname::TEXT,
                                                                        ' Q'::CHARACTER VARYING::TEXT,
                                                                        2
                                                                    ),
                                                                    1
                                                                ) = t.cal_qtr_1::CHARACTER VARYING::TEXT
                                                                AND (
                                                                    u.columnname::TEXT like '%SSS Program Q%'::CHARACTER VARYING::TEXT
                                                                    OR u.columnname::TEXT like '%Platinum Q%'::CHARACTER VARYING::TEXT
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
                                                AND LTRIM (
                                                    "left" (a.year_month::CHARACTER VARYING::TEXT, 4),
                                                    '0'::CHARACTER VARYING::TEXT
                                                ) = c."year"::CHARACTER VARYING::TEXT
                                                AND LTRIM (
                                                    "right" (a.year_month::CHARACTER VARYING::TEXT, 2),
                                                    '0'::CHARACTER VARYING::TEXT
                                                ) = c."month"::CHARACTER VARYING::TEXT
                                                AND c.distcode::TEXT = a.customer_code::TEXT
                                                LEFT JOIN (
                                                    SELECT edw_sku_recom.mth_mm AS year_month,
                                                        TO_DATE(
                                                            concat(edw_sku_recom.mth_mm, 15)::CHARACTER VARYING::TEXT,
                                                            'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                        ) AS DATE,
                                                        edw_sku_recom.channel_name,
                                                        edw_sku_recom.retailer_category_name,
                                                        edw_sku_recom.cust_cd AS customer_code,
                                                        "max"(edw_sku_recom.customer_name::TEXT) AS customer_name,
                                                        edw_sku_recom.class_desc AS retailer_class,
                                                        edw_sku_recom.rtruniquecode AS urc,
                                                        "max"(edw_sku_recom.retailer_name::TEXT) AS retailer_name,
                                                        edw_sku_recom.mothersku_name,
                                                        edw_sku_recom.hit_ms_flag AS msl_hit
                                                    FROM edw_sku_recom
                                                    WHERE edw_sku_recom.mothersku_name IS NOT NULL
                                                        AND substring(
                                                            edw_sku_recom.mth_mm::CHARACTER VARYING::TEXT,
                                                            0,
                                                            4
                                                        ) >= (
                                                            date_part(
                                                                year,
                                                                convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                                                            ) - 1
                                                        )::CHARACTER VARYING::TEXT
                                                    GROUP BY edw_sku_recom.mth_mm,
                                                        TO_DATE(
                                                            concat(edw_sku_recom.mth_mm, 15)::CHARACTER VARYING::TEXT,
                                                            'YYYYMMDD'::CHARACTER VARYING::TEXT
                                                        ),
                                                        edw_sku_recom.channel_name,
                                                        edw_sku_recom.retailer_category_name,
                                                        edw_sku_recom.cust_cd,
                                                        edw_sku_recom.class_desc,
                                                        edw_sku_recom.rtruniquecode,
                                                        edw_sku_recom.mothersku_name,
                                                        edw_sku_recom.hit_ms_flag
                                                ) b ON datediff (
                                                    month,
                                                    b.date::TIMESTAMP WITHOUT TIME ZONE,
                                                    a.date::TIMESTAMP WITHOUT TIME ZONE
                                                ) < 3
                                                AND datediff (
                                                    month,
                                                    b.date::TIMESTAMP WITHOUT TIME ZONE,
                                                    a.date::TIMESTAMP WITHOUT TIME ZONE
                                                ) >= 0
                                                AND a.urc::TEXT = b.urc::TEXT
                                                AND a.mothersku_name::TEXT = b.mothersku_name::TEXT
                                                AND a.channel_name::TEXT = b.channel_name::TEXT
                                                AND a.retailer_category_name::TEXT = b.retailer_category_name::TEXT
                                            GROUP BY a.year_month,
                                                a.channel_name,
                                                a.customer_code,
                                                a.customer_name,
                                                a.retailer_category_name,
                                                CASE
                                                    WHEN a.channel_name::TEXT = 'Self Service Store'::CHARACTER VARYING::TEXT THEN concat(
                                                        c.columnname,
                                                        ' ('::CHARACTER VARYING::TEXT,
                                                        c.program_name,
                                                        ')'
                                                    )::CHARACTER VARYING::TEXT
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
                                    WHERE trim(UPPER(edw_vw_ps_weights.kpi::TEXT)) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
                                        AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                                ) wt ON trim (UPPER (wt.retail_environment::TEXT)) = trim (UPPER (sku_recom.retailer_category_name::TEXT))
                                AND trim (UPPER (wt.channel::TEXT)) = trim (UPPER (sku_recom.channel_name::TEXT))
                            UNION ALL
                            SELECT 'Merchandising_Response' AS dataset,
                                msl.store_code AS customerid,
                                "max"(msl.isp_code::TEXT)::CHARACTER VARYING AS salespersonid,
                                'true' AS mustcarryitem,
                                NULL AS answerscore,
                                CASE
                                    WHEN SUM(msl.quantity) > 0::NUMERIC::NUMERIC(18, 0) THEN 'true'::CHARACTER VARYING
                                    ELSE 'false'::CHARACTER VARYING
                                END AS presence,
                                NULL AS outofstock,
                                'MSL Compliance' AS kpi,
                                TO_DATE(
                                    concat(
                                        substring (msl.yearmo::TEXT, 3, 5),
                                        '-'::CHARACTER VARYING::TEXT,
                                        "left" (msl.yearmo::TEXT, 2),
                                        '-15'::CHARACTER VARYING::TEXT
                                    ),
                                    'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                                ) AS scheduleddate,
                                'completed' AS vst_status,
                                substring(msl.yearmo::TEXT, 3, 5)::CHARACTER VARYING AS fisc_yr,
                                (
                                    concat(
                                        substring(msl.yearmo::TEXT, 3, 5),
                                        "left"(msl.yearmo::TEXT, 2)
                                    )
                                )::INTEGER AS fisc_per,
                                "max"(msl.isp_name::TEXT)::CHARACTER VARYING AS firstname,
                                '' AS lastname,
                                (
                                    concat(
                                        COALESCE(msl.store_code, 'NA'::CHARACTER VARYING)::TEXT,
                                        '-'::CHARACTER VARYING::TEXT,
                                        COALESCE(msl.store_name, 'NA'::CHARACTER VARYING)::TEXT
                                    )
                                )::CHARACTER VARYING AS customername,
                                'India' AS country,
                                msl.region AS state,
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
                                wt.weight::DOUBLE precision AS kpi_chnl_wt,
                                NULL AS ms_flag,
                                NULL AS hit_ms_flag,
                                NULL AS "y/n_flag",
                                CASE
                                    WHEN UPPER(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
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
                                    WHERE UPPER(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                                        AND UPPER(edw_vw_ps_weights.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
                                        AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                                ) wt,
                                itg_in_perfectstore_msl msl
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
                                        edw_product_key_attributes."region" as region,
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
                                ) pka ON LTRIM (
                                    msl.product_code::TEXT,
                                    '0'::CHARACTER VARYING::TEXT
                                ) = LTRIM (pka.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)
                            WHERE trim(UPPER(wt.retail_environment::TEXT)) = trim(UPPER(msl.format::TEXT))
                                AND UPPER(msl.msl::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                                AND substring(msl.yearmo::TEXT, 3, 5) >= (
                                    date_part(
                                        year,
                                        convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                                    ) - 2::DOUBLE precision
                                )::CHARACTER VARYING::TEXT
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
                                wt.weight::DOUBLE precision,
                                CASE
                                    WHEN UPPER(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
                                    ELSE 'N'::CHARACTER VARYING
                                END
                        )
                        UNION ALL
                        SELECT 'Merchandising_Response' AS dataset,
                            msl.store_code AS customerid,
                            "max"(msl.isp_code::TEXT)::CHARACTER VARYING AS salespersonid,
                            'true' AS mustcarryitem,
                            NULL AS answerscore,
                            'true' AS presence,
                            CASE
                                WHEN msl.quantity > 0::NUMERIC::NUMERIC(18, 0) THEN ''::CHARACTER VARYING
                                ELSE 'true'::CHARACTER VARYING
                            END AS outofstock,
                            'OOS Compliance' AS kpi,
                            TO_DATE(
                                to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT,
                                'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                            ) AS scheduleddate,
                            'completed' AS vst_status,
                            substring(
                                "replace" (
                                    to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT,
                                    '-'::CHARACTER VARYING::TEXT,
                                    ''::CHARACTER VARYING::TEXT
                                ),
                                0,
                                4
                            )::CHARACTER VARYING AS fisc_yr,
                            substring(
                                "replace" (
                                    to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT,
                                    '-'::CHARACTER VARYING::TEXT,
                                    ''::CHARACTER VARYING::TEXT
                                ),
                                0,
                                6
                            )::INTEGER AS fisc_per,
                            "max"(msl.isp_name::TEXT)::CHARACTER VARYING AS firstname,
                            '' AS lastname,
                            (
                                concat(
                                    coalesce(msl.store_code, 'NA'::CHARACTER VARYING)::TEXT,
                                    '-'::CHARACTER VARYING::TEXT,
                                    COALESCE(msl.store_name, 'NA'::CHARACTER VARYING)::TEXT
                                )
                            )::CHARACTER VARYING AS customername,
                            'India' AS country,
                            msl.region AS state,
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
                            wt.weight::DOUBLE precision AS kpi_chnl_wt,
                            NULL AS ms_flag,
                            NULL AS hit_ms_flag,
                            NULL AS "y/n_flag",
                            CASE
                                WHEN UPPER(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
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
                                WHERE UPPER(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                                    AND UPPER(edw_vw_ps_weights.kpi::TEXT) = 'OSA COMPLIANCE'::CHARACTER VARYING::TEXT
                                    AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                            ) wt,
                            itg_in_perfectstore_msl msl
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
                                    edw_product_key_attributes."region" as region,
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
                            ) pka ON LTRIM (
                                msl.product_code::TEXT,
                                '0'::CHARACTER VARYING::TEXT
                            ) = LTRIM (pka.matl_num::TEXT, '0'::CHARACTER VARYING::TEXT)
                        WHERE trim(UPPER(wt.retail_environment::TEXT)) = trim(UPPER(msl.format::TEXT))
                            AND date_part(
                                year,
                                TO_DATE(
                                    to_date(msl.visit_datetime)::CHARACTER VARYING::TEXT,
                                    'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                                )::TIMESTAMP WITHOUT TIME ZONE
                            ) >= (
                                date_part(
                                    year,
                                    convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                                ) - 2::DOUBLE precision
                            )
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
                            wt.weight::DOUBLE precision,
                            CASE
                                WHEN UPPER(msl.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
                                ELSE 'N'::CHARACTER VARYING
                            END
                    )
                    UNION ALL
                    SELECT 'Survey_Response' AS dataset,
                        sos.store_code AS customerid,
                        sos.isp_code AS salespersonid,
                        'true' AS mustcarryitem,
                        NULL AS answerscore,
                        NULL AS presence,
                        NULL AS outofstock,
                        'Share of Shelf' AS kpi,
                        TO_DATE(
                            to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                            'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                        ) AS scheduleddate,
                        'completed' AS vst_status,
                        substring(
                            "replace" (
                                to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                                '-'::CHARACTER VARYING::TEXT,
                                ''::CHARACTER VARYING::TEXT
                            ),
                            0,
                            4
                        )::CHARACTER VARYING AS fisc_yr,
                        substring(
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
                        (
                            concat(
                                COALESCE(sos.store_code, 'NA'::CHARACTER VARYING)::TEXT,
                                '-'::CHARACTER VARYING::TEXT,
                                COALESCE(sos.store_name, 'NA'::CHARACTER VARYING)::TEXT
                            )
                        )::CHARACTER VARYING AS customername,
                        'India' AS country,
                        sos.region AS state,
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
                        wt.weight::DOUBLE precision AS kpi_chnl_wt,
                        NULL AS ms_flag,
                        NULL AS hit_ms_flag,
                        NULL AS "y/n_flag",
                        CASE
                            WHEN UPPER(sos.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
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
                            WHERE UPPER(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                                AND UPPER(edw_vw_ps_weights.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
                                AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                        ) wt,
                        itg_in_perfectstore_sos sos
                        LEFT JOIN (
                            SELECT edw_vw_ps_targets.market,
                                edw_vw_ps_targets.kpi,
                                edw_vw_ps_targets.channel,
                                edw_vw_ps_targets.retail_environment,
                                edw_vw_ps_targets.attribute_1,
                                edw_vw_ps_targets.attribute_2,
                                edw_vw_ps_targets.value
                            FROM edw_vw_ps_targets
                            WHERE UPPER(edw_vw_ps_targets.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
                                AND UPPER(edw_vw_ps_targets.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                                AND UPPER(edw_vw_ps_targets.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                        ) trgt ON trgt.attribute_1::TEXT = sos.chain::TEXT
                        AND trgt.attribute_2::TEXT = sos.category::TEXT
                    WHERE trim(UPPER(wt.retail_environment::TEXT)) = trim(UPPER(sos.format::TEXT))
                        AND date_part(
                            year,
                            TO_DATE(
                                to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                                'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                            )::TIMESTAMP WITHOUT TIME ZONE
                        ) >= (
                            date_part(
                                year,
                                convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                            ) - 2::DOUBLE precision
                        )
                )
                UNION ALL
                SELECT 'Survey_Response' AS dataset,
                    sos.store_code AS customerid,
                    sos.isp_code AS salespersonid,
                    'true' AS mustcarryitem,
                    NULL AS answerscore,
                    NULL AS presence,
                    NULL AS outofstock,
                    'Share of Shelf' AS kpi,
                    TO_DATE(
                        to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                        'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                    ) AS scheduleddate,
                    'completed' AS vst_status,
                    substring(
                        "replace" (
                            to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                            '-'::CHARACTER VARYING::TEXT,
                            ''::CHARACTER VARYING::TEXT
                        ),
                        0,
                        4
                    )::CHARACTER VARYING AS fisc_yr,
                    substring(
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
                    (
                        concat(
                            COALESCE(sos.store_code, 'NA'::CHARACTER VARYING)::TEXT,
                            '-'::CHARACTER VARYING::TEXT,
                            COALESCE(sos.store_name, 'NA'::CHARACTER VARYING)::TEXT
                        )
                    )::CHARACTER VARYING AS customername,
                    'India' AS country,
                    sos.region AS state,
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
                    wt.weight::DOUBLE precision AS kpi_chnl_wt,
                    NULL AS ms_flag,
                    NULL AS hit_ms_flag,
                    NULL AS "y/n_flag",
                    CASE
                        WHEN UPPER(sos.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
                        ELSE 'N'::CHARACTER VARYING
                    END AS priority_store_flag,
                    NULL AS questiontext,
                    'DENOMINATOR' AS ques_desc,
                    sos.total_facings AS value,
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
                        WHERE UPPER(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                            AND UPPER(edw_vw_ps_weights.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
                            AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                    ) wt,
                    itg_in_perfectstore_sos sos
                    LEFT JOIN (
                        SELECT edw_vw_ps_targets.market,
                            edw_vw_ps_targets.kpi,
                            edw_vw_ps_targets.channel,
                            edw_vw_ps_targets.retail_environment,
                            edw_vw_ps_targets.attribute_1,
                            edw_vw_ps_targets.attribute_2,
                            edw_vw_ps_targets.value
                        FROM edw_vw_ps_targets
                        WHERE UPPER(edw_vw_ps_targets.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
                            AND UPPER(edw_vw_ps_targets.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                            AND UPPER(edw_vw_ps_targets.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                    ) trgt ON trgt.attribute_1::TEXT = sos.chain::TEXT
                    AND trgt.attribute_2::TEXT = sos.category::TEXT
                WHERE trim(UPPER(wt.retail_environment::TEXT)) = trim(UPPER(sos.format::TEXT))
                    AND date_part(
                        year,
                        TO_DATE(
                            to_date(sos.visit_datetime)::CHARACTER VARYING::TEXT,
                            'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                        )::TIMESTAMP WITHOUT TIME ZONE
                    ) >= (
                        date_part(
                            year,
                            convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                        ) - 2::DOUBLE precision
                    )
            )
            UNION ALL
            SELECT 'Survey_Response' AS dataset,
                prmc.store_code AS customerid,
                prmc.isp_code AS salespersonid,
                'true' AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                'Promo Compliance' AS kpi,
                TO_DATE(
                    to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT,
                    'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                ) AS scheduleddate,
                'completed' AS vst_status,
                substring(
                    "replace" (
                        to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT,
                        '-'::CHARACTER VARYING::TEXT,
                        ''::CHARACTER VARYING::TEXT
                    ),
                    0,
                    4
                )::CHARACTER VARYING AS fisc_yr,
                substring(
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
                (
                    concat(
                        COALESCE(prmc.store_code, 'NA'::CHARACTER VARYING)::TEXT,
                        '-'::CHARACTER VARYING::TEXT,
                        COALESCE(prmc.store_name, 'NA'::CHARACTER VARYING)::TEXT
                    )
                )::CHARACTER VARYING AS customername,
                'India' AS country,
                PRMC.region AS state,
                PRMC.chain AS storereference,
                PRMC.format AS storetype,
                'MT' AS channel,
                PRMC.chain AS salesgroup,
                'India' AS prod_hier_l1,
                NULL AS prod_hier_l2,
                prmc.product_category AS prod_hier_l3,
                prmc.product_brand AS prod_hier_l4,
                NULL AS prod_hier_l5,
                NULL AS prod_hier_l6,
                NULL AS prod_hier_l7,
                NULL AS prod_hier_l8,
                NULL AS prod_hier_l9,
                wt.weight::DOUBLE precision AS kpi_chnl_wt,
                NULL AS ms_flag,
                NULL AS hit_ms_flag,
                CASE
                    WHEN UPPER(prmc.ispromotionavailable::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'YES'::CHARACTER VARYING
                    ELSE 'NO'::CHARACTER VARYING
                END AS "y/n_flag",
                CASE
                    WHEN UPPER(prmc.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
                    ELSE 'N'::CHARACTER VARYING
                END AS priority_store_flag,
                (
                    concat(
                        'Is the '::CHARACTER VARYING::TEXT,
                        COALESCE(
                            prmc.promotion_product_name,
                            ''::CHARACTER VARYING
                        )::TEXT,
                        ' '::CHARACTER VARYING::TEXT,
                        COALESCE(prmc.promotionoffertype, ''::CHARACTER VARYING)::TEXT,
                        ' promotion available?'::CHARACTER VARYING::TEXT
                    )
                )::CHARACTER VARYING (255) AS questiontext,
                NULL AS ques_desc,
                NULL AS value,
                NULL AS mkt_share,
                (
                    concat(
                        COALESCE(prmc.notavailablereason, ''::CHARACTER VARYING)::TEXT,
                        ' '::CHARACTER VARYING::TEXT,
                        COALESCE(prmc.price_off, ''::CHARACTER VARYING)::TEXT
                    )
                )::CHARACTER VARYING AS rej_reason,
                prmc.photopath AS photo_url
            FROM (
                    SELECT itg_in_perfectstore_promo.store_code,
                        itg_in_perfectstore_promo.store_name,
                        itg_in_perfectstore_promo.isp_code,
                        itg_in_perfectstore_promo.isp_name,
                        ITG_IN_PERFECTSTORE_PROMO.region,
                        ITG_IN_PERFECTSTORE_PROMO.chain,
                        ITG_IN_PERFECTSTORE_PROMO.format,
                        itg_in_perfectstore_promo.product_category,
                        itg_in_perfectstore_promo.product_brand,
                        "max"(
                            to_date(itg_in_perfectstore_promo.visit_datetime)
                        ) AS visit_datetime
                    FROM itg_in_perfectstore_promo
                    GROUP BY itg_in_perfectstore_promo.store_code,
                        itg_in_perfectstore_promo.store_name,
                        itg_in_perfectstore_promo.isp_code,
                        itg_in_perfectstore_promo.isp_name,
                        ITG_IN_PERFECTSTORE_PROMO.region,
                        ITG_IN_PERFECTSTORE_PROMO.chain,
                        ITG_IN_PERFECTSTORE_PROMO.format,
                        itg_in_perfectstore_promo.product_category,
                        itg_in_perfectstore_promo.product_brand
                ) prm,
                itg_in_perfectstore_promo prmc,
                (
                    SELECT edw_vw_ps_weights.market,
                        edw_vw_ps_weights.kpi,
                        edw_vw_ps_weights.channel,
                        edw_vw_ps_weights.retail_environment,
                        edw_vw_ps_weights.weight
                    FROM edw_vw_ps_weights
                    WHERE UPPER(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                        AND UPPER(edw_vw_ps_weights.kpi::TEXT) = 'PROMO COMPLIANCE'::CHARACTER VARYING::TEXT
                        AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
                ) wt
            WHERE trim(UPPER(wt.retail_environment::TEXT)) = trim(UPPER(PRM.format::TEXT))
                AND to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::TIMESTAMP WITHOUT TIME ZONE = prmc.visit_datetime
                AND prm.store_code::TEXT = prmc.store_code::TEXT
                AND prm.store_name::TEXT = prmc.store_name::TEXT
                AND prm.isp_code::TEXT = prmc.isp_code::TEXT
                AND prm.isp_name::TEXT = prmc.isp_name::TEXT
                AND PRM.region::TEXT = PRMC.region::TEXT
                AND PRM.chain::TEXT = PRMC.chain::TEXT
                AND PRM.format::TEXT = PRMC.format::TEXT
                AND prm.product_category::TEXT = prmc.product_category::TEXT
                AND prm.product_brand::TEXT = prmc.product_brand::TEXT
                AND date_part(
                    year,
                    TO_DATE(
                        to_date(prm.visit_datetime::TIMESTAMP WITHOUT TIME ZONE)::CHARACTER VARYING::TEXT,
                        'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                    )::TIMESTAMP WITHOUT TIME ZONE
                ) >= (
                    date_part(
                        year,
                        convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                    ) - 2::DOUBLE precision
                )
        )
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
            dsp.store_code AS customerid,
            dsp.isp_code AS salespersonid,
            'true' AS mustcarryitem,
            NULL AS answerscore,
            NULL AS presence,
            NULL AS outofstock,
            'Display Compliance' AS kpi,
            TO_DATE(
                to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT,
                'YYYY-MM-DD'::CHARACTER VARYING::TEXT
            ) AS scheduleddate,
            'completed' AS vst_status,
            substring(
                "replace" (
                    to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT,
                    '-'::CHARACTER VARYING::TEXT,
                    ''::CHARACTER VARYING::TEXT
                ),
                0,
                4
            )::CHARACTER VARYING AS fisc_yr,
            substring(
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
            (
                concat(
                    COALESCE(dsp.store_code, 'NA'::CHARACTER VARYING)::TEXT,
                    '-'::CHARACTER VARYING::TEXT,
                    COALESCE(dsp.store_name, 'NA'::CHARACTER VARYING)::TEXT
                )
            )::CHARACTER VARYING AS customername,
            'India' AS country,
            dsp.region AS state,
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
            wt.weight::DOUBLE precision AS kpi_chnl_wt,
            NULL AS ms_flag,
            NULL AS hit_ms_flag,
            CASE
                WHEN UPPER(dsp.audit_status::TEXT) = 'YES'::CHARACTER VARYING::TEXT
                AND UPPER(dsp.is_available::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'YES'::CHARACTER VARYING
                ELSE 'NO'::CHARACTER VARYING
            END AS "y/n_flag",
            CASE
                WHEN UPPER(dsp.priority_store::TEXT) = 'YES'::CHARACTER VARYING::TEXT THEN 'Y'::CHARACTER VARYING
                ELSE 'N'::CHARACTER VARYING
            END AS priority_store_flag,
            (
                (
                    concat(
                        'Is the '::CHARACTER VARYING::TEXT,
                        COALESCE(dsp.posm_brand, ''::CHARACTER VARYING)::TEXT,
                        ' available?'::CHARACTER VARYING::TEXT
                    )
                )
            )::CHARACTER VARYING (255) AS questiontext,
            NULL AS ques_desc,
            NULL AS value,
            NULL AS mkt_share,
            dsp.reason AS rej_reason,
            NULL AS photo_url
        FROM itg_in_perfectstore_paid_display dsp,
            (
                SELECT edw_vw_ps_weights.market,
                    edw_vw_ps_weights.kpi,
                    edw_vw_ps_weights.channel,
                    edw_vw_ps_weights.retail_environment,
                    edw_vw_ps_weights.weight
                FROM edw_vw_ps_weights
                WHERE UPPER(edw_vw_ps_weights.channel::TEXT) = 'MT'::CHARACTER VARYING::TEXT
                    AND UPPER(edw_vw_ps_weights.kpi::TEXT) = 'DISPLAY COMPLIANCE'::CHARACTER VARYING::TEXT
                    AND UPPER(edw_vw_ps_weights.market::TEXT) = 'INDIA'::CHARACTER VARYING::TEXT
            ) wt
        WHERE trim(UPPER(wt.retail_environment::TEXT)) = trim(UPPER(dsp.format::TEXT))
            AND date_part(
                year,
                TO_DATE(
                    to_date(dsp.visit_datetime)::CHARACTER VARYING::TEXT,
                    'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                )::TIMESTAMP WITHOUT TIME ZONE
            ) >= (
                date_part(
                    year,
                    convert_timezone('UTC', current_timestamp())::TIMESTAMP WITHOUT TIME ZONE
                ) - 2::DOUBLE precision
            )
    ) v_rpt_in_perfect_store
WHERE v_rpt_in_perfect_store.channel::TEXT = 'MT'::TEXT
),
ind_2 as (
    SELECT UPPER(sku_agg.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    sku_agg.customerid,
    sku_agg.salespersonid,
    NULL AS visitid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    NULL AS survey_enddate,
    NULL AS questionkey,
    NULL AS questiontext,
    NULL AS valuekey,
    NULL AS value,
    NULL AS productid,
    UPPER(sku_agg.mustcarryitem::TEXT)::CHARACTER VARYING AS mustcarryitem,
    NULL AS answerscore,
    UPPER(sku_agg.presence::TEXT)::CHARACTER VARYING AS presence,
    sku_agg.outofstock,
    NULL AS mastersurveyname,
    UPPER(sku_agg.kpi::TEXT)::CHARACTER VARYING AS kpi,
    NULL AS category,
    NULL AS segment,
    NULL AS vst_visitid,
    sku_agg.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(sku_agg.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    sku_agg.fisc_yr::INTEGER AS fisc_yr,
    sku_agg.fisc_per,
    sku_agg.firstname,
    sku_agg.lastname,
    NULL AS cust_remotekey,
    sku_agg.customername,
    sku_agg.country,
    sku_agg.state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    UPPER(sku_agg.storereference::TEXT)::CHARACTER VARYING (255) AS storereference,
    sku_agg.storetype,
    sku_agg.channel,
    sku_agg.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    NULL AS brand,
    NULL AS productname,
    NULL AS eannumber,
    NULL AS matl_num,
    sku_agg.prod_hier_l1,
    sku_agg.prod_hier_l2,
    sku_agg.prod_hier_l3,
    sku_agg.prod_hier_l4,
    sku_agg.prod_hier_l5,
    sku_agg.prod_hier_l6,
    sku_agg.prod_hier_l7,
    sku_agg.prod_hier_l8,
    sku_agg.prod_hier_l9,
    sku_agg.kpi_chnl_wt,
    NULL AS mkt_share,
    NULL AS ques_desc,
    (
        row_number() OVER (
            PARTITION BY sku_agg.customerid,
            sku_agg.fisc_per
            order by null
        )
    )::CHARACTER VARYING AS "y/n_flag",
    NULL AS posm_execution_flag,
    NULL AS rej_reason,
    NULL AS response,
    NULL AS response_score,
    NULL AS acc_rej_reason,
    CASE
        WHEN COALESCE(str_agg.hit_ms_flag, 0::NUMERIC) >= (0.35 * COALESCE(str_agg.ms_flag, 0::NUMERIC)) THEN '1'::TEXT
        ELSE '0'::TEXT
    END::CHARACTER VARYING AS actual,
    '1' AS "target",
    'Y' AS priority_store_flag,
    sku_agg.photo_url,
    NULL AS website_url,
    NULL as store_grade
FROM rpt_in_perfect_store sku_agg
    LEFT JOIN (
        SELECT rpt_in_perfect_store.customerid,
            rpt_in_perfect_store.salespersonid,
            rpt_in_perfect_store.kpi,
            rpt_in_perfect_store.scheduleddate,
            rpt_in_perfect_store.fisc_yr,
            rpt_in_perfect_store.fisc_per,
            rpt_in_perfect_store.customername,
            rpt_in_perfect_store.firstname,
            rpt_in_perfect_store.country,
            rpt_in_perfect_store.state,
            rpt_in_perfect_store.storereference,
            rpt_in_perfect_store.storetype,
            rpt_in_perfect_store.channel,
            rpt_in_perfect_store.salesgroup,
            SUM(
                rpt_in_perfect_store.hit_ms_flag::NUMERIC::NUMERIC(18, 0)
            ) AS hit_ms_flag,
            SUM(
                rpt_in_perfect_store.ms_flag::NUMERIC::NUMERIC(18, 0)
            ) AS ms_flag
        FROM rpt_in_perfect_store
        WHERE UPPER(rpt_in_perfect_store.channel::TEXT) <> 'MT'::TEXT
        GROUP BY rpt_in_perfect_store.customerid,
            rpt_in_perfect_store.salespersonid,
            rpt_in_perfect_store.kpi,
            rpt_in_perfect_store.scheduleddate,
            rpt_in_perfect_store.fisc_yr,
            rpt_in_perfect_store.fisc_per,
            rpt_in_perfect_store.customername,
            rpt_in_perfect_store.firstname,
            rpt_in_perfect_store.country,
            rpt_in_perfect_store.state,
            rpt_in_perfect_store.storereference,
            rpt_in_perfect_store.storetype,
            rpt_in_perfect_store.channel,
            rpt_in_perfect_store.salesgroup
    ) str_agg ON str_agg.customerid::TEXT = sku_agg.customerid::TEXT
    AND str_agg.scheduleddate = sku_agg.scheduleddate
    AND str_agg.fisc_yr::TEXT = sku_agg.fisc_yr::TEXT
    AND str_agg.fisc_per = sku_agg.fisc_per
    AND str_agg.salespersonid::TEXT = sku_agg.salespersonid::TEXT
WHERE UPPER(sku_agg.channel::TEXT) <> 'MT'::TEXT
),
vnm as (
    SELECT UPPER(v_rpt_vn_perfect_store.dataset::TEXT)::CHARACTER VARYING AS dataset,
    NULL AS merchandisingresponseid,
    NULL AS surveyresponseid,
    v_rpt_vn_perfect_store.customerid,
    v_rpt_vn_perfect_store.salespersonid,
    NULL AS visitid,
    NULL AS mrch_resp_startdt,
    NULL AS mrch_resp_enddt,
    NULL AS mrch_resp_status,
    NULL AS mastersurveyid,
    NULL AS survey_status,
    NULL AS survey_enddate,
    NULL AS questionkey,
    v_rpt_vn_perfect_store.questiontext,
    NULL AS valuekey,
    v_rpt_vn_perfect_store.value::CHARACTER VARYING AS value,
    NULL AS productid,
    v_rpt_vn_perfect_store.mustcarryitem,
    -- 	(case when (v_rpt_vn_perfect_store.mustcarryitem = true) then 'TRUE' 
    -- 	     when (v_rpt_vn_perfect_store.mustcarryitem = false) then 'FALSE' 
    --  	end)   AS mustcarryitem,
    NULL AS answerscore,
    v_rpt_vn_perfect_store.presence,
    v_rpt_vn_perfect_store.outofstock,
    --  (case when (v_rpt_vn_perfect_store.presence = true) then 'TRUE' 
    -- 	     when (v_rpt_vn_perfect_store.presence = false) then 'FALSE'
    --       end)  AS presence,
    -- 	(case when (v_rpt_vn_perfect_store.outofstock = true) then 'TRUE' 
    -- 	      when (v_rpt_vn_perfect_store.outofstock = false) then 'FALSE' 
    -- 	 end) AS outofstock,
    NULL AS mastersurveyname,
    UPPER(v_rpt_vn_perfect_store.kpi::TEXT)::CHARACTER VARYING AS kpi,
    v_rpt_vn_perfect_store.category AS category,
    v_rpt_vn_perfect_store.segment AS segment,
    NULL AS vst_visitid,
    v_rpt_vn_perfect_store.scheduleddate,
    NULL AS scheduledtime,
    NULL AS duration,
    UPPER(v_rpt_vn_perfect_store.vst_status::TEXT)::CHARACTER VARYING AS vst_status,
    v_rpt_vn_perfect_store.fisc_yr::INTEGER AS fisc_yr,
    v_rpt_vn_perfect_store.fisc_per::INTEGER AS fisc_per,
    v_rpt_vn_perfect_store.firstname,
    v_rpt_vn_perfect_store.lastname,
    NULL AS cust_remotekey,
    v_rpt_vn_perfect_store.customername,
    v_rpt_vn_perfect_store.country,
    v_rpt_vn_perfect_store.state,
    NULL AS county,
    NULL AS district,
    NULL AS city,
    v_rpt_vn_perfect_store.storereference,
    v_rpt_vn_perfect_store.storetype,
    v_rpt_vn_perfect_store.channel,
    v_rpt_vn_perfect_store.salesgroup,
    NULL AS bu,
    NULL AS soldtoparty,
    v_rpt_vn_perfect_store.prod_hier_l5 AS brand,
    v_rpt_vn_perfect_store.prod_hier_l9 AS productname,
    NULL AS eannumber,
    NULL AS matl_num,
    v_rpt_vn_perfect_store.prod_hier_l1,
    v_rpt_vn_perfect_store.prod_hier_l2,
    v_rpt_vn_perfect_store.prod_hier_l3,
    v_rpt_vn_perfect_store.prod_hier_l4,
    v_rpt_vn_perfect_store.prod_hier_l5,
    v_rpt_vn_perfect_store.prod_hier_l6,
    v_rpt_vn_perfect_store.prod_hier_l7,
    v_rpt_vn_perfect_store.prod_hier_l8,
    v_rpt_vn_perfect_store.prod_hier_l9,
    v_rpt_vn_perfect_store.kpi_chnl_wt,
    v_rpt_vn_perfect_store.mkt_share,
    UPPER(v_rpt_vn_perfect_store.ques_desc::TEXT)::CHARACTER VARYING AS ques_desc,
    v_rpt_vn_perfect_store."y/n_flag",
    NULL AS posm_execution_flag,
    NULL AS rej_reason,
    NULL AS response,
    NULL AS response_score,
    NULL AS acc_rej_reason,
    NULL AS actual,
    NULL AS "target",
    priority_store_flag,
    NULL AS photo_url,
    NULL AS website_url,
    NULL AS store_grade
FROM v_rpt_vn_perfect_store
),
final as (
    select * from rex
    union all
    select * from pop6
    union all
    select * from pcf
    union all
    select * from phl
    union all
    select * from idn
    union all
    select * from jpn
    union all
    select * from sgp
    union all
    select * from tha_1
    union all
    select * from tha_2
    union all
    select * from mys_1
    union all
    select * from mys_2
    union all
    select * from ind_1
    union all
    select * from ind_2
    union all
    select * from vnm
)
select * from final