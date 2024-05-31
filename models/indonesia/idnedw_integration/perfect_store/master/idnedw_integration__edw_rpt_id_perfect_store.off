with edw_id_ps_msl_osa as (
    select * from -- ref('idnedw_integration__edw_id_ps_msl_osa') 
                snapidnedw_integration.edw_id_ps_msl_osa
),       
itg_id_ps_msl_reference as (
    select * from -- ref('idnitg_integration__itg_id_ps_msl_reference') 
                        idnitg_integration.itg_id_ps_msl_reference
),
edw_id_ps_outlet_master as (
    select * from -- ref('idnedw_integration__edw_id_ps_outlet_master') 
                snapidnedw_integration.edw_id_ps_outlet_master
),
edw_id_ps_merchandiser_master as (
    select * from -- ref('idnedw_integration__edw_id_ps_merchandiser_master') 
                snapidnedw_integration.edw_id_ps_merchandiser_master
),
edw_vw_ps_weights  as (
    select * from {{ ref('aspedw_integration__edw_vw_ps_weights') }}
),
itg_mds_id_5ps_store_mapping as (
    select * from {{ ref('idnitg_integration__itg_mds_id_5ps_store_mapping') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_id_ps_product_master as (
    select * from -- ref('idnedw_integration__edw_id_ps_product_master') 
                    idnedw_integration.edw_id_ps_product_master
),
merchandising_response_transformed_1 as (
    SELECT 
            'Merchandising_Response' as dataset
            ,msl.outlet_id as customerid
            ,msl.merchandiser_id as salespersonid
            ,null as mrch_resp_startdt
            ,null as mrch_resp_enddt
            ,null as survey_enddate
            ,null as questiontext
            ,null as value
            ,msl.mustcarryitem
            ,null as answerscore
            ,msl.presence
            ,null as outofstock
            ,null as onshelfavailability
            ,'MSL Compliance' AS kpi
            ,msl.input_date as scheduleddate
            ,'completed' AS vst_status
            ,time_dim.year as fisc_yr
            ,time_dim.mnth_id as fisc_per
            ,(msl.merchandiser_id || ' - ' || split_part(mst.merchandiser_name, ' ', 1)) as firstname
            ,substring(mst.merchandiser_name, length(split_part(mst.merchandiser_name, ' ', 1)) + 2, length(mst.merchandiser_name)) as lastname
            ,pom.outlet_name as customername
            ,'Indonesia' AS country
            ,pom.cust_group AS storereference
            ,pom.channel AS storetype
            ,'MT' AS channel
            ,pom.channel_group AS local_channel_group
            ,str_map.distributor_name AS salesgroup
            ,msl.sub_brand AS franchise
            ,msl.brand AS brand
            ,msl.sku_variant AS variant
            ,'NA' AS putup
            ,weight.weight AS kpi_chnl_wt
            ,NULL AS mkt_share
            ,NULL AS share_of_shelf
            ,NULL AS ques_desc
            ,NULL AS "y/n_flag"
            ,NULL AS promo_desc
            ,NULL AS product_cmp_competitor_jnj
            ,NULL AS photo_link
            ,NULL AS posm_execution_flag
            ,0 AS stock_qty_pcs
            ,0 AS qty_min
            ,NULL AS question_code
            ,NULL AS aq_channel_name
            ,NULL AS rej_reason
            ,str_map.region AS STATE
            ,NULL AS city
            ,'Y' AS priority_store_flag
        FROM (
            SELECT outlet_id
                ,input_date
                ,cust_group
                ,channel
                ,merchandiser_id
                ,sku_variant
                ,brand
                ,sub_brand
                ,'true' AS mustcarryitem
                ,CASE 
                    WHEN MAX(msl_flag) = 1
                        THEN 'true'
                    ELSE 'false'
                    END AS presence
            FROM (
                SELECT main.outlet_id
                    ,main.input_date
                    ,main.cust_group
                    ,main.channel
                    ,main.merchandiser_id
                    ,main.sku
                    ,main.sku_variant
                    ,main.brand
                    ,main.sub_brand
                    ,CASE 
                        WHEN flag.put_up_sku IS NOT NULL
                            THEN 1
                        ELSE 0
                        END AS msl_flag
                FROM (
                    SELECT edw.outlet_id
                        ,edw.input_date
                        ,edw.cust_group
                        ,edw.channel
                        ,edw.merchandiser_id
                        ,ref.sku
                        ,ref.sku_variant
                        ,ref.brand
                        ,ref.sub_brand
                    FROM (
                        SELECT DISTINCT outlet_id
                            ,input_date
                            ,cust_group
                            ,channel
                            ,merchandiser_id
                        FROM edw_id_ps_msl_osa
                        ) edw
                    JOIN (
                        SELECT DISTINCT sku
                            ,sku_variant
                            ,brand
                            ,sub_brand
                            ,cust_group
                            ,channel
                        FROM itg_id_ps_msl_reference
                        ) ref ON UPPER(edw.cust_group) = UPPER(ref.cust_group)
                        AND UPPER(edw.channel) = UPPER(ref.channel)
                    ) main
                LEFT JOIN (
                    SELECT DISTINCT outlet_id
                        ,merchandiser_id
                        ,cust_group
                        ,channel
                        ,put_up_sku
                        ,input_date
                    FROM edw_id_ps_msl_osa
                    ) flag ON UPPER(main.sku) = UPPER(flag.put_up_sku)
                    AND UPPER(main.cust_group) = UPPER(flag.cust_group)
                    AND main.input_date = flag.input_date
                    AND UPPER(main.channel) = UPPER(flag.channel)
                    AND main.outlet_id = flag.outlet_id
                    AND main.merchandiser_id = flag.merchandiser_id
                )
            GROUP BY outlet_id
                ,input_date
                ,cust_group
                ,channel
                ,merchandiser_id
                ,sku_variant
                ,brand
                ,sub_brand
            ) msl
        LEFT JOIN edw_id_ps_outlet_master pom ON msl.outlet_id = pom.outlet_id
        LEFT JOIN edw_id_ps_merchandiser_master mst ON msl.merchandiser_id = mst.merchandiser_id
        LEFT JOIN (
            SELECT DISTINCT channel
                ,weight AS weight
                ,kpi
            FROM edw_vw_ps_weights
            WHERE UPPER(kpi) = 'MCS'
                AND UPPER(channel) = 'MT'
                AND UPPER(market) = 'INDONESIA'
            ) weight ON CASE 
                WHEN UPPER(weight.channel) = 'MT'
                    THEN 1
                ELSE 0
                END = 1
        LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON msl.outlet_id = str_map.store_id
        LEFT JOIN (
            SELECT DISTINCT edw_vw_os_time_dim."year" as year
                ,edw_vw_os_time_dim.mnth_id
                ,edw_vw_os_time_dim.cal_date
            FROM edw_vw_os_time_dim
            ) time_dim ON msl.input_date = time_dim.cal_date
            AND YEAR(msl.input_date) >= YEAR(convert_timezone('UTC', current_timestamp()))-2
),
merchandising_response_transformed_2 as (
    SELECT 
        'Merchandising_Response' AS dataset
		,osa.outlet_id AS customerid
		,osa.merchandiser_id AS salespersonid
		,NULL AS mrch_resp_startdt
		,NULL AS mrch_resp_enddt
		,NULL AS survey_enddate
		,NULL AS questiontext
		,NULL AS value
		,'true' AS mustcarryitem
		,NULL AS answerscore
		,'true' AS presence
		,CASE 
			WHEN osa.stock_qty_pcs < ref.qty_min
				THEN 'true'
			ELSE ''
			END AS outofstock
		,CASE 
			WHEN osa.stock_qty_pcs >= ref.qty_min
				THEN 'true'
			ELSE 'false'
			END AS onshelfavailability
		,'OOS Compliance' AS kpi
		,osa.input_date AS scheduleddate
		,'completed' AS vst_status
		,time_dim.year AS fisc_yr
		,time_dim.mnth_id AS fisc_per
		,(osa.merchandiser_id || ' - ' || SPLIT_PART(mst.merchandiser_name, ' ', 1)) AS firstname
		,SUBSTRING(mst.merchandiser_name, LENGTH(SPLIT_PART(mst.merchandiser_name, ' ', 1)) + 2, LENGTH(mst.merchandiser_name)) AS lastname
		,pom.outlet_name AS customername
		,'Indonesia' AS country
		,pom.cust_group AS storereference
		,pom.channel AS storetype
		,'MT' AS channel
		,pom.channel_group AS local_channel_group
		,str_map.distributor_name AS salesgroup
		,ppm.franchise AS franchise
		,ppm.brand AS brand
		,ppm.sku_variant AS variant
		,osa.put_up_sku AS putup
		,weight.weight AS kpi_chnl_wt
		,NULL AS mkt_share
		,NULL AS share_of_shelf
		,NULL AS ques_desc
		,NULL AS "y/n_flag"
		,NULL AS promo_desc
		,NULL AS product_cmp_competitor_jnj
		,NULL AS photo_link
		,NULL AS posm_execution_flag
		,osa.stock_qty_pcs
		,ref.qty_min
		,NULL AS question_code
		,NULL AS aq_channel_name
		,NULL AS rej_reason
		,str_map.region AS STATE
		,NULL AS city
		,'Y' AS priority_store_flag
	FROM edw_id_ps_msl_osa osa
	LEFT JOIN edw_id_ps_outlet_master pom ON osa.outlet_id = pom.outlet_id
	LEFT JOIN edw_id_ps_merchandiser_master mst ON osa.merchandiser_id = mst.merchandiser_id
	LEFT JOIN (
		SELECT DISTINCT pm.sku
			,pm.sku_variant
			,pm.franchise
			,pm.brand
			,pmsl.cust_group
			,pmsl.channel_group
			,pmsl.channel
		FROM edw_id_ps_product_master pm
		JOIN itg_id_ps_msl_reference pmsl ON UPPER(pm.sku) = UPPER(pmsl.sku)
			AND UPPER(pm.sku_variant) = UPPER(pmsl.sku_variant)
			AND UPPER(pm.brand) = UPPER(pmsl.brand)
		) ppm ON UPPER(osa.put_up_sku) = UPPER(ppm.sku)
		AND UPPER(osa.cust_group) = UPPER(ppm.cust_group)
		AND UPPER(osa.channel) = UPPER(ppm.channel)
		AND UPPER(osa.franchise) = UPPER(ppm.franchise)
	LEFT JOIN (
		SELECT DISTINCT sku
			,cust_group
			,channel
			,qty_min
		FROM itg_id_ps_msl_reference
		WHERE UPPER(identifier) IN (
				'MCS & OSA NKA - TOP CHAIN'
				,'OSA LKA'
				)
		) ref ON UPPER(osa.put_up_sku) = UPPER(ref.sku)
		AND UPPER(osa.cust_group) = UPPER(ref.cust_group)
		AND UPPER(osa.channel) = UPPER(ref.channel)
	LEFT JOIN (
		SELECT channel
			,weight AS weight
			,kpi
		FROM edw_vw_ps_weights
		WHERE UPPER(kpi) = 'OSA'
			AND UPPER(channel) = 'MT'
			AND UPPER(market) = 'INDONESIA'
		) weight ON CASE 
			WHEN UPPER(weight.channel) = 'MT'
				THEN 1
			ELSE 0
			END = 1
	LEFT JOIN itg_mds_id_5ps_store_mapping str_map ON osa.outlet_id = str_map.store_id
	LEFT JOIN (
		SELECT DISTINCT edw_vw_os_time_dim."year" as year
			,edw_vw_os_time_dim.mnth_id
			,edw_vw_os_time_dim.cal_date
		FROM edw_vw_os_time_dim
		) time_dim ON osa.input_date = time_dim.cal_date
		AND YEAR(osa.input_date) >= YEAR(convert_timezone('UTC', current_timestamp()))-2
	WHERE ref.qty_min IS NOT NULL
)
select * from merchandising_response_transformed_1
union all
select * from merchandising_response_transformed_2

        