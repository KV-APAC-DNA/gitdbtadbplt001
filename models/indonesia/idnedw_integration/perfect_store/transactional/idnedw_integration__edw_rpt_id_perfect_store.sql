with edw_id_ps_msl_osa as
(
    select * from {{ref('idnedw_integration__edw_id_ps_msl_osa')}}
),
itg_id_ps_msl_reference as
(
    select * from {{ref('idnitg_integration__itg_id_ps_msl_reference')}}
),
edw_id_ps_outlet_master as
(
    select * from {{ref('idnedw_integration__edw_id_ps_outlet_master')}}
),
edw_id_ps_merchandiser_master as
(
    select * from {{ref('idnedw_integration__edw_id_ps_merchandiser_master')}}
),
edw_id_ps_product_master as
(
    select * from {{ref('idnedw_integration__edw_id_ps_product_master')}}
),
edw_id_ps_promotion as
(
    select * from {{ref('idnedw_integration__edw_id_ps_promotion')}}
),
edw_id_ps_planogram as
(
    select * from {{ref('idnedw_integration__edw_id_ps_planogram')}}
),
edw_id_ps_brand_blocking as
(
    select * from {{ref('idnedw_integration__edw_id_ps_brand_blocking')}}
),
edw_id_ps_visibility as
(
    select * from {{ref('idnedw_integration__edw_id_ps_visibility')}}
),
edw_indonesia_noo_analysis as
(
    select * from {{ref('idnedw_integration__edw_indonesia_noo_analysis')}}
),
edw_distributor_ivy_merchandising as
(
    select * from {{ref('idnedw_integration__edw_distributor_ivy_merchandising')}}
),
edw_time_dim as
(
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
edw_distributor_ivy_outlet_master as
(
    select * from {{ref('idnedw_integration__edw_distributor_ivy_outlet_master')}}
),
edw_distributor_customer_dim as
(
    select * from {{ref('idnedw_integration__edw_distributor_customer_dim')}}
),
edw_distributor_dim as
(
    select * from {{ref('idnedw_integration__edw_distributor_dim')}}
),
itg_mds_id_5ps_store_mapping as
(
    select * from {{ref('idnitg_integration__itg_mds_id_5ps_store_mapping')}}
),
itg_id_ps_brand as
(
    select * from {{ref('idnitg_integration__itg_id_ps_brand')}}
),
itg_mcs_gt as
(
    select * from {{ref('idnitg_integration__itg_mcs_gt')}}
),
itg_id_ps_priority_store as
(
    select * from {{ref('idnitg_integration__itg_id_ps_priority_store')}}
),
edw_vw_ps_weights as
(
    select * from {{ref('aspedw_integration__edw_vw_ps_weights')}}
),
edw_vw_os_time_dim as
(
    select * from {{ref('sgpedw_integration__edw_vw_os_time_dim')}}
),
final as 
(
   SELECT dataset::VARCHAR(22) as dataset,
        customerid::VARCHAR(100) as customerid,
        salespersonid::VARCHAR(100) as salespersonid,
        mrch_resp_startdt::VARCHAR(1) as mrch_resp_startdt,
        mrch_resp_enddt::VARCHAR(1) as mrch_resp_enddt,
        survey_enddate::VARCHAR(1) as survey_enddate,
        questiontext::VARCHAR(500) as questiontext,
        value::VARCHAR(40) as value,
        mustcarryitem::VARCHAR(4) as mustcarryitem,
        answerscore::VARCHAR(1) as answerscore,
        presence::VARCHAR(5) as presence,
        outofstock::VARCHAR(4) as outofstock,
        onshelfavailability::VARCHAR(5) as onshelfavailability,
        kpi::VARCHAR(20) as kpi,
        scheduleddate::DATE as scheduleddate,
        vst_status::VARCHAR(9) as vst_status,
        fisc_yr::NUMBER(18,0) as fisc_yr,
        fisc_per::VARCHAR(23) as fisc_per,
        firstname::VARCHAR(100) as firstname,
        lastname::VARCHAR(100) as lastname,
        substring(customername,1,100)::VARCHAR(100) AS customername,
        country::VARCHAR(9) as country,
        storereference::VARCHAR(100) as storereference,
        upper(storetype)::VARCHAR(100) as storetype,
        channel::VARCHAR(2) as channel,
        local_channel_group::VARCHAR(100) as local_channel_group,
        salesgroup::VARCHAR(200) as salesgroup,
        franchise::VARCHAR(510) as franchise,
        brand::VARCHAR(510) as brand,
        variant::VARCHAR(2000) as variant,
        putup::VARCHAR(100) as putup,
        kpi_chnl_wt::NUMBER(38,5) as kpi_chnl_wt,
        mkt_share::NUMBER(38,5) as mkt_share,
        share_of_shelf::NUMBER(38,5) as share_of_shelf,
        ques_desc::VARCHAR(11) as ques_desc,
        "y/n_flag"::VARCHAR(150) as "y/n_flag",
        promo_desc::VARCHAR(500) as promo_desc,
        product_cmp_competitor_jnj::VARCHAR(510) as product_cmp_competitor_jnj,
        photo_link::VARCHAR(500) as photo_link,
        posm_execution_flag::VARCHAR(7) as posm_execution_flag,
        stock_qty_pcs::NUMBER(18,0) as stock_qty_pcs,
        qty_min::NUMBER(18,0) as qty_min,
        question_code::VARCHAR(50) as question_code,
        aq_channel_name::VARCHAR(50) as aq_channel_name,
        rej_reason::VARCHAR(500) as rej_reason,
        state::VARCHAR(256) as state,
        city::VARCHAR(256) as city,
        priority_store_flag::VARCHAR(10) as priority_store_flag
    FROM ( SELECT 'Merchandising_Response' AS dataset,
                msl.outlet_id AS customerid,
                msl.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                msl.mustcarryitem,
                NULL AS answerscore,
                msl.presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'MSL Compliance' AS kpi,
                msl.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (msl.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                msl.sub_brand AS franchise,
                msl.brand AS brand,
                msl.sku_variant AS variant,
                'NA' AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                NULL AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                NULL AS photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT outlet_id,
                    input_date,
                    cust_group,
                    channel,
                    merchandiser_id,
                    sku_variant,
                    brand,
                    sub_brand,
                    'true' AS mustcarryitem,
                    CASE
                        WHEN MAX(msl_flag) = 1 THEN 'true'
                        ELSE 'false'
                    END AS presence
                FROM (SELECT main.outlet_id,
                            main.input_date,
                            main.cust_group,
                            main.channel,
                            main.merchandiser_id,
                            main.sku,
                            main.sku_variant,
                            main.brand,
                            main.sub_brand,
                            CASE
                            WHEN flag.put_up_sku IS NOT NULL THEN 1
                            ELSE 0
                            END AS msl_flag
                    FROM (SELECT edw.outlet_id,
                                edw.input_date,
                                edw.cust_group,
                                edw.channel,
                                edw.merchandiser_id,
                                ref.sku,
                                ref.sku_variant,
                                ref.brand,
                                ref.sub_brand
                            FROM (SELECT DISTINCT outlet_id,
                                        input_date,
                                        cust_group,
                                        channel,
                                        merchandiser_id
                                FROM edw_id_ps_msl_osa) edw
                            JOIN (SELECT DISTINCT sku,
                                        sku_variant,
                                        brand,
                                        sub_brand,
                                        cust_group,
                                        channel
                                    FROM itg_id_ps_msl_reference) ref
                                ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                            AND UPPER (edw.channel) = UPPER (ref.channel)) main
                        LEFT JOIN (SELECT DISTINCT outlet_id,
                                        merchandiser_id,
                                        cust_group,
                                        channel,
                                        put_up_sku,
                                        input_date
                                FROM edw_id_ps_msl_osa) flag
                            ON UPPER (main.sku) = UPPER (flag.put_up_sku)
                            AND UPPER (main.cust_group) = UPPER (flag.cust_group)
                            AND main.input_date = flag.input_date
                            AND UPPER (main.channel) = UPPER (flag.channel)
                            AND main.outlet_id = flag.outlet_id
                            AND main.merchandiser_id = flag.merchandiser_id)
                GROUP BY outlet_id,
                        input_date,
                        cust_group,
                        channel,
                        merchandiser_id,
                        sku_variant,
                        brand,
                        sub_brand) msl
            LEFT JOIN edw_id_ps_outlet_master pom ON msl.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON msl.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT DISTINCT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'MCS'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON msl.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON msl.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,msl.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        UNION ALL
        SELECT 'Merchandising_Response' AS dataset,
                osa.outlet_id AS customerid,
                osa.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                'true' AS mustcarryitem,
                NULL AS answerscore,
                'true' AS presence,
                CASE
                WHEN osa.stock_qty_pcs < ref.qty_min THEN 'true'
                ELSE ''
                END AS outofstock,
                CASE
                WHEN osa.stock_qty_pcs >= ref.qty_min THEN 'true'
                ELSE 'false'
                END AS onshelfavailability,
                'OOS Compliance' AS kpi,
                osa.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (osa.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                ppm.franchise AS franchise,
                ppm.brand AS brand,
                ppm.sku_variant AS variant,
                osa.put_up_sku AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                NULL AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                NULL AS photo_link,
                NULL AS posm_execution_flag,
                osa.stock_qty_pcs,
                ref.qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM edw_id_ps_msl_osa osa
            LEFT JOIN edw_id_ps_outlet_master pom ON osa.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON osa.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT DISTINCT pm.sku,
                            pm.sku_variant,
                            pm.franchise,
                            pm.brand,
                            pmsl.cust_group,
                            pmsl.channel_group,
                            pmsl.channel
                    FROM edw_id_ps_product_master pm
                        JOIN itg_id_ps_msl_reference pmsl
                        ON UPPER (pm.sku) = UPPER (pmsl.sku)
                        AND UPPER (pm.sku_variant) = UPPER (pmsl.sku_variant)
                        AND UPPER (pm.brand) = UPPER (pmsl.brand)) ppm
                ON UPPER (osa.put_up_sku) = UPPER (ppm.sku)
                AND UPPER (osa.cust_group) = UPPER (ppm.cust_group)
                AND UPPER (osa.channel) = UPPER (ppm.channel)
                AND UPPER (osa.franchise) = UPPER (ppm.franchise)
            LEFT JOIN (SELECT DISTINCT sku,
                            cust_group,
                            channel,
                            qty_min
                    FROM itg_id_ps_msl_reference
                    WHERE UPPER(identifier) IN ('MCS & OSA NKA - TOP CHAIN','OSA LKA')) ref
                ON UPPER (osa.put_up_sku) = UPPER (ref.sku)
                AND UPPER (osa.cust_group) = UPPER (ref.cust_group)
                AND UPPER (osa.channel) = UPPER (ref.channel)
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'OSA'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON osa.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON osa.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,osa.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        WHERE ref.qty_min IS NOT NULL
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                pro.outlet_id AS customerid,
                pro.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                promo_desc AS questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Promo Compliance' AS kpi,
                pro.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (pro.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                NULL AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                CASE
                WHEN KEY IS NOT NULL THEN 'YES'
                ELSE 'NO'
                END AS "y/n_flag",
                promo_desc,
                NULL AS product_cmp_competitor_jnj,
                photo_link,
                UPPER(posm_execution_flag) AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT msl_trans.input_date,
                    msl_trans.outlet_id,
                    msl_trans.merchandiser_id,
                    promo_trans.promo_desc,
                    promo_trans.photo_link,
                    promo_trans.posm_execution_flag,
                    promo_trans.outlet_id||promo_trans.merchandiser_id||to_char(promo_trans.input_date,'yyyymmdd') AS KEY
                FROM (SELECT DISTINCT edw.outlet_id,
                            edw.input_date,
                            edw.merchandiser_id
                    FROM (SELECT DISTINCT outlet_id,
                                input_date,
                                cust_group,
                                channel,
                                merchandiser_id
                            FROM edw_id_ps_msl_osa) edw
                        JOIN (SELECT DISTINCT cust_group,
                                    channel
                            FROM itg_id_ps_msl_reference) ref
                        ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                        AND UPPER (edw.channel) = UPPER (ref.channel)) msl_trans
                LEFT JOIN edw_id_ps_promotion promo_trans
                        ON msl_trans.input_date = promo_trans.input_date
                        AND msl_trans.outlet_id = promo_trans.outlet_id
                        AND msl_trans.merchandiser_id = promo_trans.merchandiser_id) pro
            LEFT JOIN edw_id_ps_outlet_master pom ON pro.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON pro.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'PROMO'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON pro.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON pro.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,pro.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                pgm.outlet_id AS customerid,
                pgm.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Planogram Compliance' AS kpi,
                pgm.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (pgm.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                pgm.franchise AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                CASE
                WHEN KEY IS NOT NULL THEN 'YES'
                ELSE 'NO'
                END AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT msl_trans.input_date,
                    msl_trans.outlet_id,
                    msl_trans.merchandiser_id,
                    pog_trans.photo_link,
                    pog_trans.franchise,
                    pog_trans.outlet_id||pog_trans.merchandiser_id||to_char(pog_trans.input_date,'yyyymmdd') AS KEY
                FROM (SELECT DISTINCT edw.outlet_id,
                            edw.input_date,
                            edw.merchandiser_id
                    FROM (SELECT DISTINCT outlet_id,
                                input_date,
                                cust_group,
                                channel,
                                merchandiser_id
                            FROM edw_id_ps_msl_osa) edw
                        JOIN (SELECT DISTINCT cust_group,
                                    channel
                            FROM itg_id_ps_msl_reference) ref
                        ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                        AND UPPER (edw.channel) = UPPER (ref.channel)) msl_trans
                LEFT JOIN edw_id_ps_planogram pog_trans
                        ON msl_trans.input_date = pog_trans.input_date
                        AND msl_trans.outlet_id = pog_trans.outlet_id
                        AND msl_trans.merchandiser_id = pog_trans.merchandiser_id) pgm
            LEFT JOIN edw_id_ps_outlet_master pom ON pgm.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON pgm.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'PLANOGRAM'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON pgm.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON pgm.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,pgm.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                bbg.outlet_id AS customerid,
                bbg.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Display Compliance' AS kpi,
                bbg.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (bbg.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                bbg.franchise AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                CASE
                WHEN KEY IS NOT NULL THEN 'YES'
                ELSE 'NO'
                END AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT msl_trans.input_date,
                    msl_trans.outlet_id,
                    msl_trans.merchandiser_id,
                    display_trans.photo_link,
                    display_trans.franchise,
                    display_trans.outlet_id||display_trans.merchandiser_id||to_char(display_trans.input_date,'yyyymmdd') AS KEY
                FROM (SELECT DISTINCT edw.outlet_id,
                            edw.input_date,
                            edw.merchandiser_id
                    FROM (SELECT DISTINCT outlet_id,
                                input_date,
                                cust_group,
                                channel,
                                merchandiser_id
                            FROM edw_id_ps_msl_osa) edw
                        JOIN (SELECT DISTINCT cust_group,
                                    channel
                            FROM itg_id_ps_msl_reference) ref
                        ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                        AND UPPER (edw.channel) = UPPER (ref.channel)) msl_trans
                LEFT JOIN edw_id_ps_brand_blocking display_trans
                        ON msl_trans.input_date = display_trans.input_date
                        AND msl_trans.outlet_id = display_trans.outlet_id
                        AND msl_trans.merchandiser_id = display_trans.merchandiser_id) bbg
            LEFT JOIN edw_id_ps_outlet_master pom ON bbg.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON bbg.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'BRAND BLOCKING'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON bbg.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON bbg.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,bbg.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        UNION ALL
        SELECT DISTINCT 'Survey_Response' AS dataset,
                vby.outlet_id AS customerid,
                vby.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                vby.number_of_facing AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Share of Shelf' AS kpi,
                vby.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (vby.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                vby.franchise AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                vby.threshold_reference AS mkt_share,
                vby.share_of_shelf AS share_of_shelf,
                'numerator' AS ques_desc,
                NULL AS "y/n_flag",
                NULL AS promo_desc,
                IPB.rg_brand AS product_cmp_competitor_jnj,
                photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM edw_id_ps_visibility vby
            JOIN (SELECT DISTINCT franchise,
                        brand,
                        rg_brand
                FROM itg_ID_PS_Brand
                WHERE UPPER(jj_brand) = 'Y') IPB
            ON UPPER (vby.product_cmp_competitor_jnj) = UPPER (IPB.brand)
            AND UPPER (vby.franchise) = UPPER (IPB.franchise)
            LEFT JOIN edw_id_ps_outlet_master pom ON vby.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON vby.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(KPI) = 'FAIRSHARE'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON vby.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON vby.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,vby.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        UNION ALL
        SELECT DISTINCT 'Survey_Response' AS dataset,
                vby.outlet_id AS customerid,
                vby.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                CAST(vby1.number_of_facing AS VARCHAR) AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Share of Shelf' AS kpi,
                vby.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (vby.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                vby.franchise AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                vby.threshold_reference AS mkt_share,
                vby.share_of_shelf AS share_of_shelf,
                'denominator' AS ques_desc,
                NULL AS "y/n_flag",
                NULL AS promo_desc,
                IIPB.rg_brand AS product_cmp_competitor_jnj,
                photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM edw_id_ps_visibility vby
            LEFT JOIN (SELECT outlet_id,
                            input_date,
                            franchise,
                            merchandiser_id,
                            SUM(number_of_facing) number_of_facing
                    FROM (SELECT DISTINCT visibility.outlet_id,
                                    visibility.input_date,
                                    visibility.franchise,
                                    visibility.merchandiser_id,
                                    ipb.rg_brand,
                                    visibility.number_of_facing
                            FROM (SELECT outlet_id,
                                        input_date,
                                        franchise,
                                        merchandiser_id,
                                        product_cmp_competitor_jnj,
                                        MIN(number_of_facing) number_of_facing
                                FROM edw_id_ps_visibility
                                GROUP BY outlet_id,
                                            input_date,
                                            franchise,
                                            merchandiser_id,
                                            product_cmp_competitor_jnj) AS visibility
                            LEFT JOIN (SELECT DISTINCT franchise,
                                                brand,
                                                rg_brand
                                        FROM itg_ID_PS_Brand) IPB
                                    ON UPPER (visibility.franchise) = UPPER (ipb.franchise)
                                    AND UPPER (visibility.product_cmp_competitor_jnj) = UPPER (ipb.brand))
                    WHERE rg_brand IS NOT NULL
                    GROUP BY outlet_id,
                                input_date,
                                franchise,
                                merchandiser_id) vby1
                ON vby.outlet_id = vby1.outlet_id
                AND vby.input_date = vby1.input_date
                AND vby.merchandiser_id = vby1.merchandiser_id
                AND UPPER (vby.franchise) = UPPER (vby1.franchise)
            LEFT JOIN edw_id_ps_outlet_master pom ON vby.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON vby.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT DISTINCT franchise,
                            brand,
                            rg_brand
                    FROM itg_ID_PS_Brand) IIPB
                ON UPPER (vby.product_cmp_competitor_jnj) = UPPER (IIPB.brand)
                AND UPPER (vby.franchise) = UPPER (IIPB.franchise)
            LEFT JOIN (SELECT channel,
                            weight  AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(KPI) = 'FAIRSHARE'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON vby.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON vby.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,vby.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        WHERE IIPB.rg_brand IS NOT NULL
        ---------------------- to get data for additional outlets in Promo/Display/Planogram ---------------------------
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                pro.outlet_id AS customerid,
                pro.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                promo_desc AS questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Promo Compliance' AS kpi,
                pro.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (pro.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                NULL AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                'YES' AS "y/n_flag",
                promo_desc,
                NULL AS product_cmp_competitor_jnj,
                photo_link,
                UPPER(posm_execution_flag) AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT promo_trans.input_date,
                    promo_trans.outlet_id,
                    promo_trans.merchandiser_id,
                    promo_trans.promo_desc,
                    promo_trans.photo_link,
                    promo_trans.posm_execution_flag,
                    msl_trans.outlet_id||msl_trans.merchandiser_id||to_char(msl_trans.input_date,'yyyymmdd') AS KEY
                FROM edw_id_ps_promotion promo_trans
                LEFT JOIN (SELECT DISTINCT edw.outlet_id,
                                    edw.input_date,
                                    edw.merchandiser_id
                            FROM (SELECT DISTINCT outlet_id,
                                        input_date,
                                        cust_group,
                                        channel,
                                        merchandiser_id
                                FROM edw_id_ps_msl_osa) edw
                            JOIN (SELECT DISTINCT cust_group,
                                            channel
                                    FROM itg_id_ps_msl_reference) ref
                                ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                                AND UPPER (edw.channel) = UPPER (ref.channel)) msl_trans
                        ON promo_trans.input_date = msl_trans.input_date
                        AND promo_trans.outlet_id = msl_trans.outlet_id
                        AND promo_trans.merchandiser_id = msl_trans.merchandiser_id) pro
            LEFT JOIN edw_id_ps_outlet_master pom ON pro.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON pro.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight  AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'PROMO'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON pro.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON pro.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,pro.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        WHERE pro.KEY IS NULL
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                pgm.outlet_id AS customerid,
                pgm.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Planogram Compliance' AS kpi,
                pgm.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (pgm.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                pgm.franchise AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                'YES' AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT pog_trans.input_date,
                    pog_trans.outlet_id,
                    pog_trans.merchandiser_id,
                    pog_trans.photo_link,
                    pog_trans.franchise,
                    msl_trans.outlet_id||msl_trans.merchandiser_id||to_char(msl_trans.input_date,'yyyymmdd') AS KEY
                FROM edw_id_ps_planogram pog_trans
                LEFT JOIN (SELECT DISTINCT edw.outlet_id,
                                    edw.input_date,
                                    edw.merchandiser_id
                            FROM (SELECT DISTINCT outlet_id,
                                        input_date,
                                        cust_group,
                                        channel,
                                        merchandiser_id
                                FROM edw_id_ps_msl_osa) edw
                            JOIN (SELECT DISTINCT cust_group,
                                            channel
                                    FROM itg_id_ps_msl_reference) ref
                                ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                                AND UPPER (edw.channel) = UPPER (ref.channel)) msl_trans
                        ON pog_trans.input_date = msl_trans.input_date
                        AND pog_trans.outlet_id = msl_trans.outlet_id
                        AND pog_trans.merchandiser_id = msl_trans.merchandiser_id) pgm
            LEFT JOIN edw_id_ps_outlet_master pom ON pgm.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON pgm.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'PLANOGRAM'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON pgm.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON pgm.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,pgm.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        WHERE pgm.KEY IS NULL
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                bbg.outlet_id AS customerid,
                bbg.merchandiser_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'Display Compliance' AS kpi,
                bbg.input_date AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                (bbg.merchandiser_id||' - '||SPLIT_PART(mst.merchandiser_name,' ',1)) AS firstname,
                SUBSTRING(mst.merchandiser_name,LENGTH(SPLIT_PART(mst.merchandiser_name,' ',1)) +2,LENGTH(mst.merchandiser_name)) AS lastname,
                pom.outlet_name AS customername,
                'Indonesia' AS country,
                pom.cust_group AS storereference,
                pom.channel AS storetype,
                'MT' AS channel,
                pom.channel_group AS local_channel_group,
                str_map.distributor_name AS salesgroup,
                bbg.franchise AS franchise,
                NULL AS brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                'YES' AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                str_map.region AS state,
                NULL AS city,
                'Y' AS priority_store_flag
        FROM (SELECT display_trans.input_date,
                    display_trans.outlet_id,
                    display_trans.merchandiser_id,
                    display_trans.photo_link,
                    display_trans.franchise,
                    msl_trans.outlet_id||msl_trans.merchandiser_id||to_char(msl_trans.input_date,'yyyymmdd') AS KEY
                FROM edw_id_ps_brand_blocking display_trans
                LEFT JOIN (SELECT DISTINCT edw.outlet_id,
                                    edw.input_date,
                                    edw.merchandiser_id
                            FROM (SELECT DISTINCT outlet_id,
                                        input_date,
                                        cust_group,
                                        channel,
                                        merchandiser_id
                                FROM edw_id_ps_msl_osa) edw
                            JOIN (SELECT DISTINCT cust_group,
                                            channel
                                    FROM itg_id_ps_msl_reference) ref
                                ON UPPER (edw.cust_group) = UPPER (ref.cust_group)
                                AND UPPER (edw.channel) = UPPER (ref.channel)) msl_trans
                        ON display_trans.input_date = msl_trans.input_date
                        AND display_trans.outlet_id = msl_trans.outlet_id
                        AND display_trans.merchandiser_id = msl_trans.merchandiser_id) bbg
            LEFT JOIN edw_id_ps_outlet_master pom ON bbg.outlet_id = pom.outlet_id
            LEFT JOIN edw_id_ps_merchandiser_master mst ON bbg.merchandiser_id = mst.merchandiser_id
            LEFT JOIN (SELECT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'BRAND BLOCKING'
                    AND   UPPER(channel) = 'MT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'MT' THEN 1 ELSE 0 END = 1
            LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON bbg.outlet_id = str_map.store_id
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON bbg.input_date = time_dim.cal_date
                AND DATE_PART (YEAR,bbg.input_date) >= (DATE_PART (YEAR,sysdate()) -2)
        WHERE bbg.KEY IS NULL
        UNION ALL
        ----------------------------GT MSL Integration -----------------------------------
    SELECT dataset,
        customerid,
        salespersonid,
        mrch_resp_startdt,
        mrch_resp_enddt,
        survey_enddate,
        questiontext,
        value,
        mustcarryitem,
        answerscore,
        presence,
        outofstock,
        onshelfavailability,
        kpi,
        scheduleddate,
        vst_status,
        fisc_yr,
        fisc_per,
        firstname,
        lastname,
        FIRST_VALUE(customername) OVER (PARTITION BY TRIM(SPLIT_PART(customername,'-',1)) ORDER BY dataset ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS customername,
        country,
        storereference,
        storetype,
        channel,
        local_channel_group,
        salesgroup,
        franchise,
        brand,
        variant,
        putup,
        kpi_chnl_wt,
        mkt_share,
        share_of_shelf,
        ques_desc,
        "y/n_flag",
        promo_desc,
        product_cmp_competitor_jnj,
        photo_link,
        posm_execution_flag,
        stock_qty_pcs,
        qty_min,
        question_code,
        aq_channel_name,
        rej_reason,
        state,
        city,
        priority_store_flag
    from (SELECT DISTINCT 'Merchandising_Response' AS dataset,
                msl.cust_id_map AS customerid,
                msl.slsmn_id AS salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                NULL AS questiontext,
                NULL AS value,
                'true' AS mustcarryitem,
                NULL AS answerscore,
                CASE
                WHEN ref.niv > 0.00 THEN 'true'
                ELSE 'false'
                END AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                'MSL Compliance' AS kpi,
                TO_DATE((msl.year_month|| '-15'),'YYYY-MM-DD') AS scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                CASE WHEN (LENGTH(TRIM(SPLIT_PART(msl.slsmn_nm,'-',1)))=14 AND LENGTH(TRIM(SPLIT_PART(msl.slsmn_nm,'.',2)))=6) then 
    (TRIM(SPLIT_PART(msl.slsmn_nm,'-',1))||' - '||TRIM(SPLIT_PART(msl.slsmn_nm,' ',3))) 
    ELSE (msl.slsmn_id||' - '||SPLIT_PART(msl.slsmn_nm,' ',1)) END AS firstname,
    CASE WHEN (LENGTH(TRIM(SPLIT_PART(msl.slsmn_nm,'-',1)))=14 AND LENGTH(TRIM(SPLIT_PART(msl.slsmn_nm,'.',2)))=6) then 
    SUBSTRING(TRIM(SPLIT_PART(msl.slsmn_nm,'-',2)),LENGTH(SPLIT_PART(TRIM(SPLIT_PART(msl.slsmn_nm,'-',2)),' ',1)) +2,LENGTH(TRIM(SPLIT_PART(msl.slsmn_nm,'-',1))))
    ELSE SUBSTRING(msl.slsmn_nm,LENGTH(SPLIT_PART(msl.slsmn_nm,' ',1)) +2,LENGTH(msl.slsmn_nm)) END AS lastname,
                (msl.jjid||' - '||msl.cust_nm_map) AS customername,
                'Indonesia' AS country,
                msl.dstrbtr_grp_nm AS storereference,
                msl.tiering AS storetype,
                'GT' AS channel,
                msl.chnl_grp AS local_channel_group,
                msl.jj_sap_dstrbtr_nm AS salesgroup,
                product.franchise,
                product.brand,
                msl.sku_name AS variant,
                'NA' AS putup,
                weight.weight AS kpi_chnl_wt,
                CAST(NULL AS NUMERIC(38,5)) AS mkt_share,
                CAST(NULL AS NUMERIC(38,5)) AS share_of_shelf,
                NULL AS ques_desc,
                NULL AS "y/n_flag",
                NULL AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                NULL AS photo_link,
                NULL AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                NULL AS question_code,
                NULL AS aq_channel_name,
                NULL AS rej_reason,
                msl.region AS state,
                msl.area AS city,
                CASE
                WHEN  UPPER (PSF.jjid) IS NOT NULL THEN 'Y'
                ELSE 'N'
                END AS priority_store_flag
        FROM (SELECT DISTINCT TO_CHAR(TO_DATE(img.year||img.month,'YYYYMMMM'),'YYYY-MM') AS year_month,
                    img.tiering,
                    img.sku_name,
                    store.jj_sap_dstrbtr_id,
                    store.cust_id_map,
                    store.jjid,
                    store.slsmn_id,
                    store.slsmn_nm,
                    store.cust_nm_map,
                    store.dstrbtr_grp_nm,
                    store.jj_sap_dstrbtr_nm,
                    store.chnl,
                    store.chnl_grp,
                    store.region,
                    store.area
                FROM itg_mcs_gt img
                LEFT JOIN (WITH master_ref AS
                            (
                            SELECT DISTINCT edw_vw_os_time_dim."year",
                                    SUBSTRING(edw_vw_os_time_dim.cal_date,1,7) AS L1_month,
                                    SUBSTRING(to_date(add_months (edw_vw_os_time_dim.cal_date,-2)),1,7) AS L3_month
                            FROM edw_vw_os_time_dim
                            )
                            SELECT DISTINCT master_ref.L1_month,
                                    master_ref.L3_month,
                                    edw.jj_sap_dstrbtr_id,
                                    edw.cust_id_map,
                                    edw.jjid,
                                    edw.slsmn_id,
                                    edw.slsmn_nm,
                                    first_value(edw.cust_nm_map) over (partition by edw.jjid order by to_date(edw.bill_dt) desc rows between unbounded preceding and unbounded following) as cust_nm_map,
                                    edw.dstrbtr_grp_nm,
                                    edw.jj_sap_dstrbtr_nm,
                                    edw.chnl,
                                    edw.chnl_grp,
                                    edw.region,
                                    edw.area,
                                    COALESCE(edw.tiering,'NA') AS tiering
                            FROM edw_indonesia_noo_analysis edw,
                                master_ref
                            WHERE SUBSTRING(to_date(edw.bill_dt),1,7) >= master_ref.L3_month
                            AND   SUBSTRING(to_date(edw.bill_dt),1,7) <= master_ref.L1_month
                            AND   UPPER(edw.mcs_status) = 'Y') store
                        ON TO_CHAR (TO_DATE (img.year||img.month,'YYYYMMMM'),'YYYY-MM') = store.L1_month
                        AND COALESCE (UPPER (img.tiering),'NA') = COALESCE (UPPER (store.tiering),'NA')) msl
            LEFT JOIN (SELECT L1_month,
                            L3_month,
                            jj_sap_dstrbtr_id,
                            cust_id_map,
                            slsmn_id,
                            slsmn_nm,
                            cust_nm_map,
                            dstrbtr_grp_nm,
                            jj_sap_dstrbtr_nm,
                            chnl,
                            chnl_grp,
                            region,
                            area,
                            franchise,
                            brand,
                            tiering,
                            local_variant,
                            mcs_status,
                            niv
                    FROM ( WITH date_ref
                    AS
                    (SELECT DISTINCT edw_vw_os_time_dim."year",
                            SUBSTRING(edw_vw_os_time_dim.cal_date,1,7) AS L1_month,
                            SUBSTRING(to_date(add_months (edw_vw_os_time_dim.cal_date,-2)),1,7) AS L3_month
                    FROM edw_vw_os_time_dim) 
                            SELECT DISTINCT date_ref.L1_month,date_ref.L3_month,edw.jj_sap_dstrbtr_id,edw.cust_id_map,edw.slsmn_id,edw.jj_sap_prod_id,
                            edw.slsmn_nm,edw.cust_nm_map,edw.dstrbtr_grp_nm,edw.jj_sap_dstrbtr_nm,edw.chnl,edw.chnl_grp,edw.region,edw.area,edw.franchise,edw.brand,edw.tiering,edw.local_variant,edw.mcs_status,SUM(edw.niv) niv 
                            FROM edw_indonesia_noo_analysis edw,date_ref
                    WHERE SUBSTRING(to_date(edw.bill_dt),1,7) >= date_ref.L3_month
                    AND   SUBSTRING(to_date(edw.bill_dt),1,7) <= date_ref.L1_month
                    GROUP BY date_ref.L1_month,
                                date_ref.L3_month,
                                edw.jj_sap_dstrbtr_id,
                                edw.cust_id_map,
                                edw.slsmn_id,
                                edw.jj_sap_prod_id,
                                edw.slsmn_nm,
                                edw.cust_nm_map,
                                edw.dstrbtr_grp_nm,
                                edw.jj_sap_dstrbtr_nm,
                                edw.chnl,
                                edw.chnl_grp,
                                edw.region,
                                edw.area,
                                edw.franchise,
                                edw.brand,
                                edw.tiering,
                                edw.local_variant,
                                edw.mcs_status) WHERE UPPER(mcs_status) = 'Y' ) ref
                ON msl.year_month = ref.L1_month
                AND COALESCE (UPPER (msl.cust_id_map),'NA') = COALESCE (UPPER (ref.cust_id_map),'NA')
                AND COALESCE (UPPER (msl.sku_name),'NA') = COALESCE (UPPER (ref.local_variant),'NA')
                AND COALESCE (UPPER (msl.jj_sap_dstrbtr_id),'NA') = COALESCE (UPPER (ref.jj_sap_dstrbtr_id),'NA')
    AND COALESCE (UPPER (msl.slsmn_id),'NA') = COALESCE (UPPER (ref.slsmn_id),'NA')
        
            LEFT JOIN (SELECT DISTINCT franchise,
                            brand,
                            local_variant
                    FROM edw_indonesia_noo_analysis) product ON COALESCE (UPPER (msl.sku_name),'NA') = COALESCE (UPPER (product.local_variant),'NA')
            LEFT JOIN (SELECT DISTINCT channel,
                            weight AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) = 'MCS'
                    AND   UPPER(channel) = 'GT'
                    AND  UPPER(market)='INDONESIA') weight ON CASE WHEN UPPER (weight.channel) = 'GT' THEN 1 ELSE 0 END = 1
                    LEFT JOIN (SELECT DISTINCT jjid from itg_id_ps_priority_store) PSF 
                    ON UPPER (msl.jjid) = UPPER (PSF.jjid)
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON TO_DATE ( (msl.year_month|| '-15'),'YYYY-MM-DD') = time_dim.cal_date
                AND DATE_PART (YEAR,TO_DATE ( (msl.year_month|| '-15'),'YYYY-MM-DD')) >= (DATE_PART (YEAR,sysdate()) -2)
                WHERE msl.year_month<=(select MAX(SUBSTRING(to_date(bill_dt),1,7)) FROM edw_indonesia_noo_analysis)
        UNION ALL
        SELECT 'Survey_Response' AS dataset,
                mer.customerid,
                mer.salespersonid,
                NULL AS mrch_resp_startdt,
                NULL AS mrch_resp_enddt,
                NULL AS survey_enddate,
                mer.questiontext,
                NULL AS value,
                NULL AS mustcarryitem,
                NULL AS answerscore,
                NULL AS presence,
                NULL AS outofstock,
                NULL AS onshelfavailability,
                CASE
                WHEN UPPER(SUBSTRING(mer.question_code,3,1)) = 'P' THEN 'Promo Compliance'
                WHEN UPPER(SUBSTRING(mer.question_code,3,1)) = 'D' THEN 'Display Compliance'
                END AS kpi,
                mer.scheduleddate,
                'completed' AS vst_status,
                time_dim."year" AS fisc_yr,
                time_dim.mnth_id AS fisc_per,
                mer.firstname,
                mer.lastname,
                mer.customername,
                'Indonesia' AS country,
                mer.storereference,
                mer.storetype,
                'GT' AS channel,
                mer.local_channel_group,
                mer.salesgroup,
                PB.franchise,
                PB.rg_brand,
                NULL AS variant,
                NULL AS putup,
                weight.weight AS kpi_chnl_wt,
                NULL AS mkt_share,
                NULL AS share_of_shelf,
                NULL AS ques_desc,
                CASE
                WHEN UPPER(mer.srd_answer) IN ('YES','NO') THEN UPPER(mer.srd_answer)
                WHEN UPPER(mer.srd_answer) IN ('TIDAK MENJUAL PRODUCT') THEN 'NO'
                ELSE 'NA'
                END AS "y/n_flag",
                SPLIT_PART(mer.questiontext,'^',2) AS promo_desc,
                NULL AS product_cmp_competitor_jnj,
                mer.photo_link,
                CASE
                WHEN UPPER(SUBSTRING(mer.question_code,3,1)) = 'P' THEN 'Y'
                ELSE NULL
                END AS posm_execution_flag,
                0 AS stock_qty_pcs,
                0 AS qty_min,
                mer.question_code,
                UPPER(SUBSTRING(mer.question_code,1,2)) AS aq_channel_name,
                CASE
                WHEN UPPER(mer.srd_answer) IN ('TIDAK MENJUAL PRODUCT') THEN UPPER(mer.srd_answer)
                ELSE 'NULL'
                END AS rej_reason,
                mer.region AS state,
                mer.area AS city,
                CASE
                WHEN  UPPER (PSF.jjid) IS NOT NULL THEN 'Y'
                ELSE 'N'
                END AS priority_store_flag
        FROM (SELECT distributor_code,
        customerid,
        jjid,
        salespersonid,
        scheduleddate,
        questiontext,
        question_code,
        srd_answer,
        photo_link,
        FIRST_VALUE(customername) OVER (PARTITION BY TRIM(SPLIT_PART(customername,'-',1)) ORDER BY identifier ASC NULLS LAST ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following) AS customername,
        storereference,
        storetype,
        local_channel_group,
        salesgroup,
        firstname,
        lastname,
        region,
        area
    FROM (WITH mer_ref AS
    (
    SELECT DISTINCT distributor_code,
            retailer_code AS customerid,
            retailer_name AS customername,
            sales_repcode AS salespersonid,
            sales_repname AS slsmn_nm,
            surveydate AS scheduleddate,
            aq_name AS questiontext,
            SPLIT_PART(aq_name,'^',1) AS question_code,
            srd_answer,
            link AS photo_link,
            ETD.jj_mnth_id as jj_mnth_id 
    FROM edw_distributor_ivy_merchandising IVYM left join edw_time_dim ETD on to_date(IVYM.surveydate)= to_date(ETD.cal_date)
    WHERE UPPER(SUBSTRING(aq_name,3,1)) IN ('P','D')
    )
    SELECT DISTINCT 'M1' AS identifier,
    mer1.distributor_code,
        mer1.customerid,
        noo.jjid,
        mer1.salespersonid,
        mer1.scheduleddate,
        mer1.questiontext,
        mer1.question_code,
        mer1.srd_answer,
        mer1.photo_link,
        (noo.jjid||' - '||noo.cust_nm_map) AS customername,
        noo.dstrbtr_grp_nm AS storereference,
        noo.tiering AS storetype,
        noo.chnl_grp AS local_channel_group,
        noo.jj_sap_dstrbtr_nm AS salesgroup,
        CASE
            WHEN (LENGTH(TRIM(SPLIT_PART(noo.slsmn_nm,'-',1))) = 14 AND LENGTH(TRIM(SPLIT_PART(noo.slsmn_nm,'.',2))) = 6) THEN (TRIM(SPLIT_PART(noo.slsmn_nm,'-',1)) || ' - ' ||TRIM (SPLIT_PART(noo.slsmn_nm,' ',3)))
            ELSE (mer1.salespersonid|| ' - ' ||SPLIT_PART (noo.slsmn_nm,' ',1))
        END AS firstname,
        CASE
            WHEN (LENGTH(TRIM(SPLIT_PART(noo.slsmn_nm,'-',1))) = 14 AND LENGTH(TRIM(SPLIT_PART(noo.slsmn_nm,'.',2))) = 6) THEN SUBSTRING(TRIM(SPLIT_PART(noo.slsmn_nm,'-',2)),LENGTH(SPLIT_PART(TRIM(SPLIT_PART(noo.slsmn_nm,'-',2)),' ',1)) +2,LENGTH(TRIM(SPLIT_PART(noo.slsmn_nm,'-',1))))
            ELSE SUBSTRING(noo.slsmn_nm,LENGTH(SPLIT_PART(noo.slsmn_nm,' ',1)) +2,LENGTH(noo.slsmn_nm))
        END AS lastname,
        noo.region,
        noo.area
    FROM mer_ref mer1
    JOIN (SELECT DISTINCT jj_sap_dstrbtr_id,
                cust_id_map,
                jjid,
                first_value(cust_nm_map) over (partition by jjid order by to_date(bill_dt) desc rows between unbounded preceding and unbounded following) as cust_nm_map,
                dstrbtr_grp_nm,
                tiering,
                chnl_grp,
                jj_sap_dstrbtr_nm,
                slsmn_id,
                slsmn_nm,
                region,
                area
            FROM edw_indonesia_noo_analysis) noo 
            ON COALESCE (UPPER (mer1.distributor_code||mer1.customerid||mer1.salespersonid),'NA') = COALESCE (UPPER (noo.jj_sap_dstrbtr_id||noo.cust_id_map||noo.slsmn_id),'NA')
    UNION ALL
    SELECT DISTINCT 'M2' AS identifier,
    mer2.distributor_code,
        mer2.customerid,
        edc.jjid,
        mer2.salespersonid,
        mer2.scheduleddate,
        mer2.questiontext,
        mer2.question_code,
        mer2.srd_answer,
        mer2.photo_link,
        (edc.jjid||' - '||first_value(trim(SUBSTRING(mer2.customername,LENGTH(SPLIT_PART(mer2.customername,'-',1)) +2,LENGTH(mer2.customername)))) over (partition by edc.jjid order by to_date(mer2.scheduleddate) desc rows between unbounded preceding and unbounded following)) AS customername,
        edd.dstrbtr_grp_nm AS storereference,
        iom.cust_grp2 AS storetype,
        iom.chnl_grp AS local_channel_group,
        edd.jj_sap_dstrbtr_nm AS salesgroup,/*
        iom.cust_grp AS storereference,
        iom.outlet_type AS storetype,
        iom.chnl_grp AS local_channel_group,
        NULL AS salesgroup,*/
        TRIM(SPLIT_PART(mer2.slsmn_nm,'-',1))||' - '||TRIM(SPLIT_PART(mer2.slsmn_nm,' ',3)) AS firstname,
        SUBSTRING(TRIM(SPLIT_PART(mer2.slsmn_nm,'-',2)),LENGTH(SPLIT_PART(TRIM(SPLIT_PART(mer2.slsmn_nm,'-',2)),' ',1)) +2,LENGTH(TRIM(SPLIT_PART(mer2.slsmn_nm,'-',1)))) AS lastname,
        edd.region AS region,
        edd.area AS area
    FROM mer_ref mer2
    LEFT JOIN edw_distributor_ivy_outlet_master iom
            ON mer2.distributor_code = iom.jj_sap_dstrbtr_id
            AND UPPER (mer2.customerid) = UPPER (iom.cust_id)
    LEFT JOIN (SELECT DISTINCT jj_sap_dstrbtr_id,cust_id,jjid,effective_from,effective_to FROM  edw_distributor_customer_dim) edc
            ON mer2.distributor_code = edc.jj_sap_dstrbtr_id
            AND UPPER (mer2.customerid) = UPPER (edc.cust_id)
            and mer2.jj_mnth_id between edc.effective_from and edc.effective_to 
            LEFT JOIN (SELECT DISTINCT jj_sap_dstrbtr_id,dstrbtr_grp_nm,jj_sap_dstrbtr_nm,region,area,effective_from,effective_to FROM  edw_distributor_dim  where jj_sap_dstrbtr_id is not null) edd
            ON mer2.distributor_code = edd.jj_sap_dstrbtr_id 
            and mer2.jj_mnth_id between edd.effective_from and edd.effective_to 
    WHERE COALESCE(UPPER(mer2.distributor_code||mer2.customerid||mer2.salespersonid),'NA') NOT IN (SELECT DISTINCT COALESCE(UPPER(jj_sap_dstrbtr_id||cust_id_map||slsmn_id),'NA')
     FROM edw_indonesia_noo_analysis ) )) mer
            LEFT JOIN (SELECT DISTINCT franchise,
                            brand,
                            rg_brand
                    FROM itg_ID_PS_Brand) PB ON UPPER (SUBSTRING (mer.question_code,4,2)) = UPPER (PB.brand)
            LEFT JOIN (SELECT DISTINCT channel,
                            SUBSTRING(kpi,1,1) AS IDENTIFIER,
                            weight  AS weight,
                            kpi
                    FROM edw_vw_ps_weights
                    WHERE UPPER(kpi) IN ('PROMO','DISPLAY')
                    AND   UPPER(channel) = 'GT'
                    AND  UPPER(market)='INDONESIA') weight ON UPPER (SUBSTRING (mer.question_code,3,1)) = UPPER (weight.identifier)
                    LEFT JOIN (SELECT DISTINCT jjid from itg_id_ps_priority_store) PSF 
                    ON UPPER (mer.jjid) = UPPER (PSF.jjid)
            LEFT JOIN (SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.cal_date
                    FROM edw_vw_os_time_dim) time_dim
                ON mer.scheduleddate = time_dim.cal_date
                AND DATE_PART (YEAR,mer.scheduleddate) >= (DATE_PART (YEAR,sysdate()) -2))
    )


)
select * from final