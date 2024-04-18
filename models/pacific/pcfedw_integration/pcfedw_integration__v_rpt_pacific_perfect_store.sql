{{
    config(
        sql_header='use warehouse DEV_DNA_CORE_app2_wh;'
    )
}}

with edw_perenso_account_dim_snapshot as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_DIM_SNAPSHOT
),
EDW_PERENSO_ACCOUNT_DIM as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_ACCOUNT_DIM
),
edw_ps_msl_items as (
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PS_MSL_ITEMS
),
edw_pacific_pharmacy_dstrbtn_analysis as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_PHARMACY_DSTRBTN_ANALYSIS
),
edw_pacific_perfect_store_weights as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_PERFECT_STORE_WEIGHTS
),
edw_vw_perfect_store_trax_products as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_VW_PERFECT_STORE_TRAX_PRODUCTS
),
edw_pacific_perfect_store_targets as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_PERFECT_STORE_TARGETS
),
edw_product_key_attributes as(
select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_PRODUCT_KEY_ATTRIBUTES
),
edw_pacific_metcash_dstrbtn_analysis as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PACIFIC_METCASH_DSTRBTN_ANALYSIS
),
EDW_VW_IRI_PS_BASE_DATA as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_VW_IRI_PS_BASE_DATA
),
EDW_IRI_SCAN_SALES_AGG as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_IRI_SCAN_SALES_AGG
),
itg_trax_md_store as(
select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.itg_trax_md_store
),
edw_pacific_perenso_ims_analysis as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_pacific_perenso_ims_analysis
),
EDW_VW_PERFECT_STORE_TRAX_FACT as(
select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_VW_PERFECT_STORE_TRAX_FACT
),
msl AS (
			SELECT ean
				,retail_environment
				,msl_flag
				,valid_from
				,valid_to
				,'A' AS store_grade
			FROM edw_ps_msl_items
			WHERE store_grade_a = 'Y'
			
			UNION ALL
			
			SELECT ean
				,retail_environment
				,msl_flag
				,valid_from
				,valid_to
				,'B' AS store_grade
			FROM edw_ps_msl_items
			WHERE store_grade_b = 'Y'
			
			UNION ALL
			
			SELECT ean
				,retail_environment
				,msl_flag
				,valid_from
				,valid_to
				,'C' AS store_grade
			FROM edw_ps_msl_items
			WHERE store_grade_C = 'Y'
			
			UNION ALL
			
			SELECT ean
				,retail_environment
				,msl_flag
				,valid_from
				,valid_to
				,'D' AS store_grade
			FROM edw_ps_msl_items
			WHERE store_grade_d = 'Y'
			
			UNION ALL
			
			SELECT ean
				,retail_environment
				,msl_flag
				,valid_from
				,valid_to
				,'Not Assigned' AS store_grade
			FROM edw_ps_msl_items
			WHERE (
					store_grade_a IS NULL
					OR store_grade_a = ''
					)
				AND (
					store_grade_b IS NULL
					OR store_grade_b = ''
					)
				AND (
					store_grade_c IS NULL
					OR store_grade_c = ''
					)
				AND (
					store_grade_d IS NULL
					OR store_grade_d = ''
					)
),
acct_hist_dim AS (
			SELECT acct_id
				,acct_grade
				,CASE 
					WHEN (
							acct_grade LIKE 'A%'
							OR acct_grade LIKE '% A %'
							)
						THEN 'A'
					WHEN (
							acct_grade LIKE 'B%'
							OR acct_grade LIKE '% B %'
							)
						THEN 'B'
					WHEN (
							acct_grade LIKE 'C%'
							OR acct_grade LIKE '% C %'
							)
						AND acct_grade NOT LIKE 'Ch%'
						THEN 'C'
					WHEN acct_grade = 'Inactive Accounts'
						THEN acct_grade
					ELSE 'D'
					END AS store_grade
				,snapshot_mnth
			FROM edw_perenso_account_dim_snapshot
),
union1 as(
SELECT 'Pharmacy Distribution' AS dataset
	,-- Changed from "MERCHANDISING_RESPONSE" to "Pharmacy Distribution"
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,eppda.acct_key AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,eppda.delvry_dt AS mrch_resp_startdt
	,eppda.delvry_dt AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'Is the product listed?' AS questiontext
	,-- Changed from NULL
	NULL AS valuekey
	,NULL AS value
	,eppda.prod_key AS productid
	,CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 'TRUE'::CHARACTER VARYING
		ELSE 'FALSE'::CHARACTER VARYING
		END AS mustcarryitem
	,NULL AS answerscore
	,CASE 
		WHEN eppda.lst3_mnth_dstrbtn_flag = 0
			THEN 'FALSE'::CHARACTER VARYING
		ELSE 'TRUE'::CHARACTER VARYING
		END AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'MSL COMPLIANCE' AS kpi
	,NULL AS category
	,NULL AS segment
	,NULL AS vst_visitid
	,DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,eppda.jj_year AS fisc_yr
	,to_char(DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt))::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed the "DD" in the format
	CASE 
		WHEN upper(eppda.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(eppda.acct_terriroty::TEXT) IS NULL
			THEN 'IMS'::CHARACTER VARYING::TEXT
		ELSE split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(eppda.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(eppda.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,eppda.acct_key AS cust_remotekey
	,eppda.acct_display_name AS customername
	,CASE 
		WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
			OR eppda.acct_country IS NULL
			THEN 'Australia'::CHARACTER VARYING
		ELSE eppda.acct_country
		END AS country
	,eppda.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,eppda.acct_banner AS storereference
	,CASE 
		WHEN eppda.acct_banner::TEXT = 'Priceline Corporate'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		ELSE 'AU INDY PHARMACY'::CHARACTER VARYING
		END AS storetype
	,'Pharmacy' AS channel
	,eppda.acct_banner AS salesgroup
	,NULL AS soldtoparty
	,NULL AS brand
	,trax_product.product_local_name AS productname
	,LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,eppda.prod_sapbw_code AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Concatenated EAN with Product Name
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,-- If there is no customer level target, then use the RE level target
	NULL AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,CASE 
		WHEN eppda.lst3_mnth_dstrbtn_flag = 0
			THEN 0
		ELSE 1
		END AS actual
	,-- Changed from NULL to the same logic as "presence" column
	CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 1
		ELSE 0
		END AS target
	,-- Changed from NULL to the same logic as "mustcarryitem" column
	gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,
	--MAX(kpi_chnl_wt) AS "weight" -- Why is this needed?
	CASE 
		WHEN to_char(to_date(eppda.delvry_dt), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
			OR to_char(to_date(eppda.delvry_dt), 'YYYYMM') < '202303'
			THEN eppda.store_grade
		ELSE ahd.store_grade
		END AS store_grade
FROM (
	SELECT *
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade = 'Inactive Accounts'
				THEN acct_grade
			ELSE 'D'
			END AS store_grade
	FROM edw_pacific_pharmacy_dstrbtn_analysis
	) eppda -- Transaction table
LEFT JOIN acct_hist_dim ahd --taking store_grade history records
	ON eppda.acct_key = ahd.acct_id
	AND to_char(to_date(eppda.delvry_dt), 'YYYYMM') = ahd.snapshot_mnth
JOIN msl -- Take only EANs that exist in the MSL list by RE by time period 
	ON CASE 
		WHEN eppda.acct_banner::TEXT = 'Priceline Corporate'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		ELSE 'AU INDY PHARMACY'::CHARACTER VARYING
		END::TEXT = upper(msl.retail_environment::TEXT)
	AND ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(msl.ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND to_date(eppda.delvry_dt) >= msl.valid_from
	AND to_date(eppda.delvry_dt) <= msl.valid_to
	AND upper(msl.store_grade) = CASE 
		WHEN to_char(eppda.delvry_dt, 'YYYYMM') < 202303
			THEN 'NOT ASSIGNED'
		WHEN to_char(eppda.delvry_dt, 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
			THEN upper(eppda.store_grade)
		ELSE upper(ahd.store_grade)
		END -- Added a condition to check store_grade as well for future records
JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain MSL KPI Weights
	ON kpi_wgt.kpi_name::TEXT = 'MSL Compliance'::CHARACTER VARYING::TEXT
	AND upper(msl.retail_environment::TEXT) = upper(kpi_wgt.retail_environment::TEXT)
LEFT JOIN (
	SELECT derived_table1.ean_number
		,derived_table1.product_local_name
		,derived_table1.brand_name
		,derived_table1.category_name
		,derived_table1.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table1
	WHERE derived_table1.row_num = 1
	) trax_product -- Obtain Trax Product Hierarchy
	ON ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
------------------- Added targets to get mkt_share-------------------  
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain RE level MSL Targets by Subcategory
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND CASE 
		WHEN upper(eppda.acct_banner::TEXT) = 'Priceline Corporate'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		ELSE 'AU INDY PHARMACY'::CHARACTER VARYING
		END = upper(mkt_share_re.re_customer_value::TEXT) --storetype
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_re.sub_category::TEXT) -- need to chk
	AND upper(CASE 
			WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
				OR eppda.acct_country IS NULL
				THEN 'Australia'::CHARACTER VARYING
			ELSE eppda.acct_country
			END) = upper(mkt_share_re.market::TEXT)
	AND mkt_share_re.channel = 'Pharmacy'
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) >= to_date(mkt_share_re.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain Customer level MSL Targets by Subcategory
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT) --need to chk
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(CASE 
			WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
				OR eppda.acct_country IS NULL
				THEN 'Australia'::CHARACTER VARYING
			ELSE eppda.acct_country
			END) = upper(mkt_share_cust.market::TEXT)
	AND mkt_share_cust.channel = 'Pharmacy'
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) >=to_date(mkt_share_cust.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) <=to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Obtain GCPH Category & Subcategory for Size of the Prize Calculation
	ON c.ean_upc = ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND upper(c.ctry_nm) = upper(CASE 
			WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
				OR eppda.acct_country IS NULL
				THEN 'Australia'::CHARACTER VARYING
			ELSE eppda.acct_country
			END)
WHERE (
		upper(msl.retail_environment::TEXT) = 'AU INDY PHARMACY'::CHARACTER VARYING::TEXT
		OR upper(msl.retail_environment::TEXT) = 'BEAUTY'::CHARACTER VARYING::TEXT
		)
	AND upper(eppda.acct_banner::TEXT) <> 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_banner::TEXT) <> 'MY CHEMIST'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_banner::TEXT) <> 'CLOSED'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_display_name::TEXT) not like 'CHW%'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_display_name::TEXT) not like '%CLOSED%'::CHARACTER VARYING::TEXT
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union2 as(
SELECT 'Metcash Distribution' AS dataset
	,-- Changed from "MERCHANDISING_RESPONSE" to "Metcash Distribution"
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,eppda.acct_key AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,eppda.delvry_dt AS mrch_resp_startdt
	,eppda.delvry_dt AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'Is the product listed?' AS questiontext
	,-- Changed from NULL
	NULL AS valuekey
	,NULL AS value
	,eppda.prod_key AS productid
	,CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 'TRUE'::CHARACTER VARYING
		ELSE 'FALSE'::CHARACTER VARYING
		END AS mustcarryitem
	,NULL AS answerscore
	,CASE 
		WHEN eppda.lst3_mnth_dstrbtn_flag = 0
			THEN 'FALSE'::CHARACTER VARYING
		ELSE 'TRUE'::CHARACTER VARYING
		END AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'MSL COMPLIANCE' AS kpi
	,NULL AS category
	,NULL AS segment
	,NULL AS vst_visitid
	,DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,eppda.jj_year AS fisc_yr
	,to_char(DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt))::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed the "DD" in the format
	CASE 
		WHEN upper(eppda.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(eppda.acct_terriroty::TEXT) IS NULL
			THEN 'METCASH'::CHARACTER VARYING::TEXT
		ELSE split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(eppda.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(eppda.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,eppda.acct_key AS cust_remotekey
	,eppda.acct_display_name AS customername
	,CASE 
		WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
			OR eppda.acct_country IS NULL
			THEN 'Australia'::CHARACTER VARYING
		ELSE eppda.acct_country
		END AS country
	,eppda.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,eppda.acct_banner AS storereference
	,'INDEPENDENT GROCERY' AS storetype
	,'Grocery' AS channel
	,eppda.acct_banner AS salesgroup
	,NULL AS soldtoparty
	,NULL AS brand
	,trax_product.product_local_name AS productname
	,ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,eppda.prod_sapbw_code AS matl_num
	,'Australia' AS prod_hier_l1
	,-- Changed from NULL to "Australia"
	NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Concatenated EAN with Product Name
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,NULL AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,CASE 
		WHEN eppda.lst3_mnth_dstrbtn_flag = 0
			THEN 0
		ELSE 1
		END AS actual
	,-- Changed to same value as "presence" column
	CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 1
		ELSE 0
		END AS target
	,-- Changed to same value as "mustcarryitem" column
	gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,
	--MAX(kpi_chnl_wt) AS "weight" -- Why is this needed?
	CASE 
		WHEN cast(eppda.cal_mnth_id AS CHAR(50)) = to_char(current_timestamp(), 'YYYYMM')
			OR eppda.cal_mnth_id < 202303
			THEN eppda.store_grade
		ELSE ahd.store_grade
		END AS store_grade
FROM (
	SELECT *
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade = 'Inactive Accounts'
				THEN acct_grade
			ELSE 'D'
			END AS store_grade
	FROM edw_pacific_metcash_dstrbtn_analysis
	) eppda -- Transaction Table
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON eppda.acct_key = ahd.acct_id
	AND eppda.cal_mnth_id = ahd.snapshot_mnth
JOIN msl -- Take only EANs that exist in the MSL list by RE by time period
	ON ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(msl.ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND to_date(eppda.delvry_dt) >= msl.valid_from
	AND to_date(eppda.delvry_dt) <= msl.valid_to
	AND upper(msl.store_grade) = CASE 
		WHEN eppda.cal_mnth_id < 202303
			THEN 'NOT ASSIGNED'
		WHEN cast(eppda.cal_mnth_id AS CHAR(50)) = to_char(current_timestamp(), 'YYYYMM')
			THEN upper(nvl(eppda.store_grade, 'Not Assigned'))
		ELSE upper(nvl(ahd.store_grade, 'Not Assigned'))
		END -- Added a condition to check store_grade as well for future records
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain MSL KPI Weights by RE
	ON kpi_wgt.kpi_name::TEXT = 'MSL Compliance'::CHARACTER VARYING::TEXT
	AND upper(msl.retail_environment::TEXT) = upper(kpi_wgt.retail_environment::TEXT)
LEFT JOIN (
	SELECT derived_table2.ean_number
		,derived_table2.product_local_name
		,derived_table2.brand_name
		,derived_table2.category_name
		,derived_table2.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table2
	WHERE derived_table2.row_num = 1
	) trax_product -- Obtain Trax Product Hierarchy
	ON ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
-------------Added below to add targets----------------------
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain MSL Targets by RE by Category by Subcategory
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.re_customer_value::TEXT) = 'INDEPENDENT GROCERY' --storetype
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_re.sub_category::TEXT) -- need to chk
	AND upper(CASE 
			WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
				OR eppda.acct_country IS NULL
				THEN 'Australia'::CHARACTER VARYING
			ELSE eppda.acct_country
			END) = upper(mkt_share_re.market::TEXT)
	AND mkt_share_re.channel = 'Grocery'
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) >= to_date(mkt_share_re.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain MSL Targets by Customer by Category by Subcategory
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT) --need to chk
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(CASE 
			WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
				OR eppda.acct_country IS NULL
				THEN 'Australia'::CHARACTER VARYING
			ELSE eppda.acct_country
			END) = upper(mkt_share_cust.market::TEXT)
	AND mkt_share_cust.channel = 'Grocery'
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) >= to_date(mkt_share_cust.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) <= to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Obtain GCPH Category & Subcategory for Size of the Prize Calculation
	ON c.ean_upc = ltrim(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND upper(c.ctry_nm) = upper(CASE 
			WHEN eppda.acct_country::TEXT = 'Not Assigned'::CHARACTER VARYING::TEXT
				OR eppda.acct_country IS NULL
				THEN 'Australia'::CHARACTER VARYING
			ELSE eppda.acct_country
			END)
WHERE upper(msl.retail_environment::TEXT) = 'INDEPENDENT GROCERY'::CHARACTER VARYING::TEXT
	AND upper(eppda.acct_display_name::TEXT) not like '%CLOSED%'::CHARACTER VARYING::TEXT
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union3 as(
SELECT 'IRI Scan' AS dataset
	,-- Changed from "MERCHANDISING_RESPONSE" to "IRI Scan"
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,base.acct_key::CHARACTER VARYING AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,base.wk_end_dt AS mrch_resp_startdt
	,base.wk_end_dt AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'Is the product listed?' AS questiontext
	,-- Changed from NULL
	NULL AS valuekey
	,NULL AS value
	,sales.matl_id AS productid
	,CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 'TRUE'::CHARACTER VARYING
		ELSE 'FALSE'::CHARACTER VARYING
		END AS mustcarryitem
	,NULL AS answerscore
	,CASE 
		WHEN sales.scan_sales IS NULL
			OR sales.scan_sales = 0.0
			THEN 'FALSE'::CHARACTER VARYING
		ELSE 'TRUE'::CHARACTER VARYING
		END AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'MSL COMPLIANCE' AS kpi
	,NULL AS category
	,NULL AS segment
	,NULL AS vst_visitid
	,DATE_TRUNC('day', DATEADD('day', -10, base.wk_end_dt)) AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,sales.jj_year AS fisc_yr
	,base.jj_mnth_id::CHARACTER VARYING AS fisc_per
	,CASE 
		WHEN upper(base.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(base.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(base.acct_terriroty::TEXT) IS NULL
			THEN 'IRI'::CHARACTER VARYING::TEXT
		ELSE split_part(base.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(base.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(base.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(base.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(base.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,base.acct_key::CHARACTER VARYING AS cust_remotekey
	,base.acct_display_name AS customername
	,'Australia' AS country
	,base.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,base.acct_banner AS storereference
	,CASE 
		WHEN upper(base.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
			OR upper(base.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		WHEN upper(base.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS storetype
	,-- Also known as Retail Environment
	CASE 
		WHEN upper(base.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
			OR upper(base.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'Grocery'::CHARACTER VARYING
		WHEN upper(base.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'Pharmacy'::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS channel
	,CASE 
		WHEN upper(base.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
			THEN 'Woolworths'::CHARACTER VARYING
		WHEN upper(base.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'Coles'::CHARACTER VARYING
		WHEN upper(base.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'Chemist Warehouse & My Chemist'::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END AS salesgroup
	,-- Also know as Parent Customer
	NULL AS soldtoparty
	,NULL AS brand
	,trax_product.product_local_name AS productname
	,ltrim(base.ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,sales.matl_id AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,ltrim(base.ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Concatenated EAN with product name
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,NULL AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,CASE 
		WHEN sales.scan_sales IS NULL
			OR sales.scan_sales = 0.0
			THEN 0
		ELSE 1
		END AS actual
	,-- Changed from NULL to the same values as "presence" column
	CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 1
		ELSE 0
		END AS target
	,-- Changed from NULL to the same value as "mustcarryitem" column
	gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,CASE 
		WHEN to_char(base.wk_end_dt, 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
			OR to_char(base.wk_end_dt, 'YYYYMM') < '202303'
			THEN base.store_grade
		ELSE ahd.store_grade
		END AS store_grade
--MAX(kpi_chnl_wt) AS "weight" -- Why is this needed?
FROM (
	SELECT *
	FROM edw_vw_iri_ps_base_data
	) base -- View containing all the stores that the customer level transactions need to be propagated down to
LEFT JOIN edw_iri_scan_sales_agg sales -- Transaction table
	ON base.jj_mnth_id = sales.jj_mnth_id
	AND base.iri_market::TEXT = sales.iri_market::TEXT
	AND ltrim(base.ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(sales.iri_ean::TEXT, 0::CHARACTER VARYING::TEXT)
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON base.acct_key = ahd.acct_id
	AND to_char(base.wk_end_dt, 'YYYYMM') = ahd.snapshot_mnth
JOIN msl -- Obtain only the EANs that are present in the MSL list
	ON ltrim(base.ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(msl.ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND CASE 
		WHEN upper(sales.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
			OR upper(sales.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		WHEN upper(sales.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END::TEXT = upper(msl.retail_environment::TEXT)
	AND to_date(base.wk_end_dt::TIMESTAMP without TIME zone) >= msl.valid_from
	AND to_date(base.wk_end_dt::TIMESTAMP without TIME zone) <= msl.valid_to
	AND upper(msl.store_grade) = CASE 
		WHEN to_char(base.wk_end_dt, 'YYYYMM') < 202303
			THEN 'NOT ASSIGNED'
		WHEN to_char(base.wk_end_dt, 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
			THEN upper(base.store_grade)
		ELSE upper(ahd.store_grade)
		END -- Added a condition to check store_grade as well for future records
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain MSL KPI Weights by RE
	ON kpi_wgt.kpi_name::TEXT = 'MSL Compliance'::CHARACTER VARYING::TEXT
	AND upper(msl.retail_environment::TEXT) = upper(kpi_wgt.retail_environment::TEXT)
LEFT JOIN (
	SELECT derived_table3.ean_number
		,derived_table3.product_local_name
		,derived_table3.brand_name
		,derived_table3.category_name
		,derived_table3.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table3
	WHERE derived_table3.row_num = 1
	) trax_product -- Obtain Trax Product Hierarchy by EAN Number
	ON ltrim(base.ean::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
------------------Added below targets----------------------
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain MSL Targets by RE for the relevant time period
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND CASE 
		WHEN upper(base.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
			OR upper(base.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		WHEN upper(base.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		ELSE NULL::CHARACTER VARYING
		END = upper(mkt_share_re.re_customer_value::TEXT) --storetype
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_re.sub_category::TEXT) -- need to chk
	AND upper(mkt_share_re.market::TEXT) = 'AUSTRALIA'
	AND upper(CASE 
			WHEN upper(base.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
				OR upper(base.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
				THEN 'Grocery'::CHARACTER VARYING
			WHEN upper(base.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
				THEN 'Pharmacy'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END) = upper(mkt_share_re.channel)
	AND DATE_TRUNC('day', DATEADD('day', -10, base.wk_end_dt)) >= to_date(mkt_share_re.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, base.wk_end_dt)) <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain MSL Targets by Customer for the relevant time period
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(base.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT) --need to chk
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(mkt_share_cust.market::TEXT) = 'AUSTRALIA'
	AND upper(CASE 
			WHEN upper(base.iri_market::TEXT) = 'AU WOOLWORTHS SCAN'::CHARACTER VARYING::TEXT
				OR upper(base.iri_market::TEXT) = 'AU COLES GROUP SCAN'::CHARACTER VARYING::TEXT
				THEN 'Grocery'::CHARACTER VARYING
			WHEN upper(base.iri_market::TEXT) = 'AU MY CHEMIST GROUP SCAN'::CHARACTER VARYING::TEXT
				THEN 'Pharmacy'::CHARACTER VARYING
			ELSE NULL::CHARACTER VARYING
			END) = upper(mkt_share_cust.channel)
	AND DATE_TRUNC('day', DATEADD('day', -10, base.wk_end_dt)) >= to_date(mkt_share_cust.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, base.wk_end_dt)) <= to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Obtain GCPH Category & Subcategory for Size of the Prize Utilization
	ON c.ean_upc = ltrim(base.ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND upper(c.ctry_nm) = 'AUSTRALIA'
WHERE upper(msl.retail_environment::TEXT) = 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
	OR upper(msl.retail_environment::TEXT) = 'BIG BOX'::CHARACTER VARYING::TEXT
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union4 as(
SELECT 'Perenso NZ Foodstuffs' AS dataset
	,NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,eppda.acct_key::CHARACTER VARYING AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,eppda.delvry_dt AS mrch_resp_startdt
	,eppda.delvry_dt AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'Is the product listed?' AS questiontext
	,NULL AS valuekey
	,NULL AS value
	,eppda.prod_key::CHARACTER VARYING AS productid
	,CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING
			THEN 'TRUE'::CHARACTER VARYING
		ELSE 'FALSE'::CHARACTER VARYING
		END AS mustcarryitem
	,NULL AS answerscore
	,CASE 
		WHEN SUM(usd_nis) > 0
			THEN 'TRUE'::CHARACTER VARYING
		ELSE 'FALSE'::CHARACTER VARYING
		END AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'MSL COMPLIANCE' AS kpi
	,NULL AS category
	,NULL AS segment
	,NULL AS vst_visitid
	,to_date(eppda.delvry_dt) AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,eppda.jj_year AS fisc_yr
	,TO_CHAR(to_date(eppda.delvry_dt)::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed the "DD" in the format
	CASE 
		WHEN UPPER(eppda.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(eppda.acct_terriroty::TEXT) IS NULL
			THEN 'IMS'::CHARACTER VARYING::TEXT
		ELSE split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(eppda.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(eppda.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(eppda.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,eppda.acct_key::CHARACTER VARYING AS cust_remotekey
	,eppda.acct_display_name AS customername
	,eppda.acct_country AS country
	,eppda.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,upper(eppda.acct_banner) AS storereference
	,upper(storemas.store_type_name) AS storetype
	,'Grocery'::CHARACTER VARYING AS channel
	,upper(eppda.acct_banner) AS salesgroup
	,NULL AS soldtoparty
	,NULL AS brand
	,trax_product.product_local_name AS productname
	,LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,eppda.prod_sapbw_code AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Concatenated EAN with Product Name
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,-- If there is no customer level target, then use the RE level target
	NULL AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,CASE 
		WHEN SUM(usd_nis) > 0
			THEN 1
		ELSE 0
		END AS actual
	,CASE 
		WHEN upper(msl.msl_flag::TEXT) = 'YES'::CHARACTER VARYING::TEXT
			THEN 1
		ELSE 0
		END AS target
	,gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,
	--MAX(kpi_chnl_wt) AS "weight" -- Why is this needed?
	CASE 
		WHEN to_char(to_date(eppda.delvry_dt), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
			OR to_char(to_date(eppda.delvry_dt), 'YYYYMM') < '202303'
			THEN eppda.store_grade
		ELSE ahd.store_grade
		END AS store_grade
FROM (
	SELECT *
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade = 'Inactive Accounts'
				THEN acct_grade
			ELSE 'D'
			END AS store_grade
	FROM edw_pacific_perenso_ims_analysis
	) eppda -- Transaction table
LEFT JOIN itg_trax_md_store storemas ON storemas.store_number::TEXT = eppda.acct_key::TEXT
LEFT JOIN acct_hist_dim ahd ON ahd.acct_id = eppda.acct_key
	AND to_char(to_date(eppda.delvry_dt), 'YYYYMM') = ahd.snapshot_mnth
JOIN msl -- Take only EANs that exist in the MSL list by RE by time period
	ON UPPER(storemas.store_type_name) = UPPER(msl.retail_environment::TEXT)
	AND LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(msl.ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND to_date(eppda.delvry_dt) >= msl.valid_from
	AND to_date(eppda.delvry_dt) <= msl.valid_to
	AND upper(msl.store_grade) = CASE 
		WHEN to_char(eppda.delvry_dt, 'YYYYMM') < 202303
			THEN 'NOT ASSIGNED'
		WHEN to_char(eppda.delvry_dt, 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
			THEN upper(eppda.store_grade)
		ELSE upper(ahd.store_grade)
		END -- Added a condition to check store_grade as well for future records
JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain MSL KPI Weights
	ON UPPER(kpi_wgt.kpi_name::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND UPPER(msl.retail_environment::TEXT) = UPPER(kpi_wgt.retail_environment::TEXT)
LEFT JOIN (
	SELECT derived_table1.ean_number
		,derived_table1.product_local_name
		,derived_table1.brand_name
		,derived_table1.category_name
		,derived_table1.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table1
	WHERE derived_table1.row_num = 1
	) trax_product -- Obtain Trax Product Hierarchy
	ON LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
------------------- Added targets to get mkt_share-------------------  
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain RE level MSL Targets by Subcategory
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND UPPER(storemas.store_type_name) = UPPER(mkt_share_re.re_customer_value::TEXT) --storetype
	AND UPPER(trax_product.subcategory_local_name::TEXT) = UPPER(mkt_share_re.sub_category::TEXT) -- need to chk
	AND UPPER(eppda.acct_country) = UPPER(mkt_share_re.market::TEXT)
	AND mkt_share_re.channel = 'Grocery'
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) >= to_date(mkt_share_re.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain Customer level MSL Targets by Subcategory
	ON UPPER(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND UPPER(mkt_share_cust.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
	AND UPPER(eppda.acct_banner::TEXT) = UPPER(mkt_share_cust.re_customer_value::TEXT) --need to chk
	AND UPPER(trax_product.subcategory_local_name::TEXT) = UPPER(mkt_share_cust.sub_category::TEXT)
	AND UPPER(eppda.acct_country) = UPPER(mkt_share_cust.market::TEXT)
	AND mkt_share_cust.channel = 'Grocery'
	AND mkt_share_cust.channel = 'Grocery'
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) >= to_date(mkt_share_cust.valid_from)
	AND DATE_TRUNC('day', DATEADD('day', -10, eppda.delvry_dt)) <= to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Obtain GCPH Category & Subcategory for Size of the Prize Calculation
	ON c.ean_upc = LTRIM(eppda.prod_ean::TEXT, 0::CHARACTER VARYING::TEXT)
	AND UPPER(c.ctry_nm) = UPPER(eppda.acct_country)
WHERE UPPER(eppda.acct_banner::TEXT) <> 'CLOSED'::CHARACTER VARYING::TEXT
	AND UPPER(eppda.acct_display_name::TEXT) not like '%CLOSED%'::CHARACTER VARYING::TEXT
	AND UPPER(order_type) = 'FOODSTUFFS'
	AND UPPER(acct_country) = 'NEW ZEALAND'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,69,70,71,72
),
union5 as(
SELECT trax_fact.source AS dataset
	,-- Changed from "MERCHANDISING_RESPONSE" to "source" column in the Trax table
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,trax_fact.storeid AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,trax_fact.visit_date AS mrch_resp_startdt
	,trax_fact.visit_date AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'Is the product available on shelf?' AS questiontext
	,-- Changed from NULL to "Is the product available on shelf?"
	NULL AS valuekey
	,NULL AS value
	,NULL AS productid
	,'TRUE' AS mustcarryitem
	,-- All SKUs are marked as MSL even though they may not be in the MSL query above. This is done for the ease of calculation
	NULL AS answerscore
	,'TRUE' AS presence
	,CASE 
		WHEN trax_fact.jj_oos = 1::DOUBLE PRECISION
			THEN 'true'::CHARACTER VARYING
		ELSE ''::CHARACTER VARYING
		END AS outofstock
	,NULL AS mastersurveyname
	,'OOS COMPLIANCE' AS kpi
	,NULL AS category
	,NULL AS segment
	,NULL AS vst_visitid
	,trax_fact.visit_date AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,date_part(year, trax_fact.visit_date::TIMESTAMP without TIME zone) AS fisc_yr
	,to_char(trax_fact.visit_date::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed "DD" from the format
	CASE 
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU INDY PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BEAUTY'::CHARACTER VARYING::TEXT
				)
			THEN 'IMS'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND upper(kpi_wgt.retail_environment::TEXT) = 'INDEPENDENT GROCERY'::CHARACTER VARYING::TEXT
			THEN 'METCASH'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BIG BOX'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'NZ MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'DISCOUNTER'::CHARACTER VARYING::TEXT
				)
			THEN 'IRI'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,trax_fact.storeid AS cust_remotekey
	,acct_dim.acct_display_name AS customername
	,kpi_wgt.ctry AS country
	,acct_dim.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,acct_dim.acct_banner AS storereference
	,
	/*CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'AU INDY PHARMACY'::character varying
            ELSE trax_fact.re
        END AS storetype, */
	CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
			THEN 'AU INDY PHARMACY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		ELSE trax_fact.re
		END AS storetype
	,
	/*CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'Pharmacy'::character varying
            WHEN upper(trax_fact.re::text) = 'AU MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'INDEPENDENT GROCERY'::character varying::text OR upper(trax_fact.re::text) = 'NZ MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'DISCOUNTER'::character varying::text THEN 'Grocery'::character varying
            WHEN upper(trax_fact.re::text) = 'AU INDY PHARMACY'::character varying::text OR upper(trax_fact.re::text) = 'BEAUTY'::character varying::text OR upper(trax_fact.re::text) = 'BIG BOX'::character varying::text THEN 'Pharmacy'::character varying
            ELSE NULL::character varying
        END AS channel, */
	CASE 
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
			THEN 'Pharmacy'
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
			THEN 'Grocery'
		ELSE acct_dim.acct_type
		END AS "channel"
	,CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'MY CHEMIST'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'Chemist Warehouse & My Chemist'::CHARACTER VARYING
		ELSE acct_dim.acct_banner
		END AS salesgroup
	,NULL AS soldtoparty
	,NULL AS brand
	,trax_product.product_local_name AS productname
	,ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,NULL AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Concatenated EAN with product name
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,NULL AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,trax_fact.jj_oos AS actual
	,-- Changed from NULL to the same value as the "oos" column
	1 AS target
	,-- Changed from NULL to 1
	gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,COALESCE(CASE 
			WHEN to_char(to_date(trax_fact.visit_date), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
				OR to_char(to_date(trax_fact.visit_date), 'YYYYMM') < '202303'
				THEN acct_dim.store_grade
			ELSE ahd.store_grade
			END, 'Not Assigned') AS store_grade
--MAX(kpi_chnl_wt) AS "weight" -- Why is this needed?
FROM edw_vw_perfect_store_trax_fact trax_fact -- Transaction Table
LEFT JOIN (
	SELECT acct_dim.acct_id
		,acct_dim.acct_display_name
		,acct_dim.acct_type_desc
		,acct_dim.acct_street_1
		,acct_dim.acct_street_2
		,acct_dim.acct_street_3
		,acct_dim.acct_suburb
		,acct_dim.acct_postcode
		,acct_dim.acct_phone_number
		,acct_dim.acct_fax_number
		,acct_dim.acct_email
		,acct_dim.acct_country
		,acct_dim.acct_region
		,acct_dim.acct_state
		,acct_dim.acct_banner_country
		,acct_dim.acct_banner_division
		,acct_dim.acct_banner_type
		,acct_dim.acct_banner
		,acct_dim.acct_type
		,acct_dim.acct_sub_type
		,acct_dim.acct_grade
		,acct_dim.acct_nz_pharma_country
		,acct_dim.acct_nz_pharma_state
		,acct_dim.acct_nz_pharma_territory
		,acct_dim.acct_nz_groc_country
		,acct_dim.acct_nz_groc_state
		,acct_dim.acct_nz_groc_territory
		,acct_dim.acct_ssr_country
		,acct_dim.acct_ssr_state
		,acct_dim.acct_ssr_team_leader
		,acct_dim.acct_ssr_territory
		,acct_dim.acct_ssr_sub_territory
		,acct_dim.acct_ind_groc_country
		,acct_dim.acct_ind_groc_state
		,acct_dim.acct_ind_groc_territory
		,acct_dim.acct_ind_groc_sub_territory
		,acct_dim.acct_au_pharma_country
		,acct_dim.acct_au_pharma_state
		,acct_dim.acct_au_pharma_territory
		,acct_dim.acct_au_pharma_ssr_country
		,acct_dim.acct_au_pharma_ssr_state
		,acct_dim.acct_au_pharma_ssr_territory
		,acct_dim.acct_store_code
		,acct_dim.acct_fax_opt_out
		,acct_dim.acct_email_opt_out
		,acct_dim.acct_contact_method
		,CASE 
			WHEN upper(acct_dim.acct_ind_groc_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_ind_groc_territory
			WHEN upper(acct_dim.acct_au_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_au_pharma_territory
			WHEN upper(acct_dim.acct_nz_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_nz_pharma_territory
			ELSE 'NOT ASSIGNED'::CHARACTER VARYING
			END AS acct_terriroty
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade IS NULL
				OR acct_grade = ''
				OR upper(acct_grade) = 'NOT ASSIGNED'
				THEN 'Not Assigned'
			ELSE 'D'
			END AS store_grade
	FROM edw_perenso_account_dim acct_dim
	) acct_dim -- Obtain store attributes from store master by store code
	ON trax_fact.storeid::NUMERIC::NUMERIC(18, 0) = acct_dim.acct_id
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON acct_dim.acct_id = ahd.acct_id
	AND to_char(trax_fact.visit_date, 'YYYYMM') = ahd.snapshot_mnth
LEFT JOIN (
	SELECT derived_table4.ean_number
		,derived_table4.product_local_name
		,derived_table4.brand_name
		,derived_table4.category_name
		,derived_table4.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table4
	WHERE derived_table4.row_num = 1
	) trax_product -- Obtain Trax product hierarchy by EAN number
	ON ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain OSA KPI Weights by RE
	ON kpi_wgt.kpi_name::TEXT = 'OSA Compliance'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(kpi_wgt.retail_environment::TEXT)
------------------------Added below targets--------------------------------
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain OSA Targets by RE for the relevant time period
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'OSA COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(mkt_share_re.re_customer_value::TEXT) --storetype
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_re.sub_category::TEXT) -- need to chk
	AND upper(kpi_wgt.ctry) = upper(mkt_share_re.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_re.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_re.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain OSA Targets by Customer for the relevant time period
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'OSA COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(acct_dim.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT) --need to chk (storeref?)
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_cust.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_cust.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_cust.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Obtain GCPH Category & Subcategory for Size of the Prize Utilization
	ON c.ean_upc = ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)
	AND upper(c.ctry_nm) = upper(kpi_wgt.ctry)
WHERE trax_fact.all_oos > 0::DOUBLE PRECISION
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union6 as(
SELECT 'Trax API' AS dataset
	,-- Changed from "SURVEY_RESPONSE" to "source" column in the Trax table
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,trax_fact.storeid AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,trax_fact.visit_date AS mrch_resp_startdt
	,trax_fact.visit_date AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'Is the product promotion compliant?' AS questiontext
	,-- Changed from "trax_product.product_local_name"
	NULL AS valuekey
	,NULL AS value
	,NULL AS productid
	,NULL AS mustcarryitem
	,NULL AS answerscore
	,NULL AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'PROMO COMPLIANCE' AS kpi
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS category
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS segment
	,NULL AS vst_visitid
	,trax_fact.visit_date AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,date_part(year, trax_fact.visit_date::TIMESTAMP without TIME zone) AS fisc_yr
	,to_char(trax_fact.visit_date::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,CASE 
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU INDY PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BEAUTY'::CHARACTER VARYING::TEXT
				)
			THEN 'IMS'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND upper(kpi_wgt.retail_environment::TEXT) = 'INDEPENDENT GROCERY'::CHARACTER VARYING::TEXT
			THEN 'METCASH'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BIG BOX'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'NZ MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'DISCOUNTER'::CHARACTER VARYING::TEXT
				)
			THEN 'IRI'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,trax_fact.storeid AS cust_remotekey
	,acct_dim.acct_display_name AS customername
	,kpi_wgt.ctry AS country
	,acct_dim.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,acct_dim.acct_banner AS storereference
	,
	/*CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'AU INDY PHARMACY'::character varying
            ELSE trax_fact.re
        END AS storetype, 
        CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'Pharmacy'::character varying
            WHEN upper(trax_fact.re::text) = 'AU MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'INDEPENDENT GROCERY'::character varying::text OR upper(trax_fact.re::text) = 'NZ MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'DISCOUNTER'::character varying::text THEN 'Grocery'::character varying
            WHEN upper(trax_fact.re::text) = 'AU INDY PHARMACY'::character varying::text OR upper(trax_fact.re::text) = 'BEAUTY'::character varying::text OR upper(trax_fact.re::text) = 'BIG BOX'::character varying::text THEN 'Pharmacy'::character varying
            ELSE NULL::character varying
        END AS channel, */
	CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
			THEN 'AU INDY PHARMACY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		ELSE trax_fact.re
		END AS storetype
	,CASE 
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
			THEN 'Pharmacy'
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
			THEN 'Grocery'
		ELSE acct_dim.acct_type
		END AS "channel"
	,CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'MY CHEMIST'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'Chemist Warehouse & My Chemist'::CHARACTER VARYING
		ELSE acct_dim.acct_banner
		END AS salesgroup
	,NULL AS soldtoparty
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS brand
	,trax_product.product_local_name AS productname
	,ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,NULL AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,NULL AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Changed from NULL to EAN Number
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,NULL AS ques_desc
	,CASE 
		WHEN trax_fact.achieved = 1
			THEN 'YES'::CHARACTER VARYING
		ELSE 'NO'::CHARACTER VARYING
		END AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,trax_fact.achieved AS actual
	,-- Changed from NULL to the same value as "y/n flag"
	trax_fact.PLAN AS target
	,-- Changed from NULL to 1
	gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,COALESCE(CASE 
			WHEN to_char(to_date(trax_fact.visit_date), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
				OR to_char(to_date(trax_fact.visit_date), 'YYYYMM') < '202303'
				THEN acct_dim.store_grade
			ELSE ahd.store_grade
			END, 'Not Assigned') AS store_grade
--MAX(kpi_chnl_wt) AS "weight" -- Is this needed?
FROM (
	SELECT trans.storeid
		,trans.re
		,trans.visit_date
		,trans.productid
		,trans.jj_promo
		,trans.all_promo
		,1 AS PLAN
		,CASE 
			WHEN rowref.rownum::DOUBLE PRECISION <= trans.jj_promo
				THEN 1
			ELSE 0
			END AS achieved
	FROM edw_vw_perfect_store_trax_fact trans
	JOIN (
		SELECT row_number() OVER (order by null) AS rownum
		FROM edw_vw_perfect_store_trax_fact
		) rowref ON trans.all_promo >= rowref.rownum::DOUBLE PRECISION
	WHERE trans.all_promo > 0::DOUBLE PRECISION
	ORDER BY trans.visit_date
		,trans.storeid
		,trans.re
		,trans.productid
	) trax_fact -- Transaction table
LEFT JOIN (
	SELECT acct_dim.acct_id
		,acct_dim.acct_display_name
		,acct_dim.acct_type_desc
		,acct_dim.acct_street_1
		,acct_dim.acct_street_2
		,acct_dim.acct_street_3
		,acct_dim.acct_suburb
		,acct_dim.acct_postcode
		,acct_dim.acct_phone_number
		,acct_dim.acct_fax_number
		,acct_dim.acct_email
		,acct_dim.acct_country
		,acct_dim.acct_region
		,acct_dim.acct_state
		,acct_dim.acct_banner_country
		,acct_dim.acct_banner_division
		,acct_dim.acct_banner_type
		,acct_dim.acct_banner
		,acct_dim.acct_type
		,acct_dim.acct_sub_type
		,acct_dim.acct_grade
		,acct_dim.acct_nz_pharma_country
		,acct_dim.acct_nz_pharma_state
		,acct_dim.acct_nz_pharma_territory
		,acct_dim.acct_nz_groc_country
		,acct_dim.acct_nz_groc_state
		,acct_dim.acct_nz_groc_territory
		,acct_dim.acct_ssr_country
		,acct_dim.acct_ssr_state
		,acct_dim.acct_ssr_team_leader
		,acct_dim.acct_ssr_territory
		,acct_dim.acct_ssr_sub_territory
		,acct_dim.acct_ind_groc_country
		,acct_dim.acct_ind_groc_state
		,acct_dim.acct_ind_groc_territory
		,acct_dim.acct_ind_groc_sub_territory
		,acct_dim.acct_au_pharma_country
		,acct_dim.acct_au_pharma_state
		,acct_dim.acct_au_pharma_territory
		,acct_dim.acct_au_pharma_ssr_country
		,acct_dim.acct_au_pharma_ssr_state
		,acct_dim.acct_au_pharma_ssr_territory
		,acct_dim.acct_store_code
		,acct_dim.acct_fax_opt_out
		,acct_dim.acct_email_opt_out
		,acct_dim.acct_contact_method
		,CASE 
			WHEN upper(acct_dim.acct_ind_groc_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_ind_groc_territory
			WHEN upper(acct_dim.acct_au_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_au_pharma_territory
			WHEN upper(acct_dim.acct_nz_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_nz_pharma_territory
			ELSE 'NOT ASSIGNED'::CHARACTER VARYING
			END AS acct_terriroty
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade IS NULL
				OR acct_grade = ''
				OR upper(acct_grade) = 'NOT ASSIGNED'
				THEN 'Not Assigned'
			ELSE 'D'
			END AS store_grade
	FROM edw_perenso_account_dim acct_dim
	) acct_dim -- Obtain store attributes from Trax Store Master by store code
	ON trax_fact.storeid::NUMERIC::NUMERIC(18, 0) = acct_dim.acct_id
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON acct_dim.acct_id = ahd.acct_id
	AND to_char(trax_fact.visit_date, 'YYYYMM') = ahd.snapshot_mnth
LEFT JOIN (
	SELECT derived_table5.ean_number
		,derived_table5.product_local_name
		,derived_table5.brand_name
		,derived_table5.category_name
		,derived_table5.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table5
	WHERE derived_table5.row_num = 1
	) trax_product -- Obtain product attributes from Trax Product Master by EAN Number
	ON ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain Promo KPI Weights by RE
	ON kpi_wgt.kpi_name::TEXT = 'Promo Compliance'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(kpi_wgt.retail_environment::TEXT)
-------Added below targets--------------------------------
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain Promo Targets by RE for the relevant time period
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'PROMO COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(mkt_share_re.re_customer_value::TEXT) --storetype
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_re.sub_category::TEXT) -- need to chk
	AND upper(kpi_wgt.ctry) = upper(mkt_share_re.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_re.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_re.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain Promo Targets by Customer for the relevant time period
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'PROMO COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(acct_dim.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT) --need to chk (storeref?)
	AND UPPER(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_cust.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_cust.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_cust.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Obtain GCPH Category & Subcategory for Size of the Prize Modelling
	ON c.ean_upc = ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)
	AND upper(c.ctry_nm) = upper(kpi_wgt.ctry)
WHERE trax_fact.all_promo > 0::DOUBLE PRECISION
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union7 as(
SELECT trax_fact.source AS dataset
	,-- Changed from "SURVEY_RESPONSE" to "source" column in the Trax table
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,trax_fact.storeid AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,trax_fact.visit_date AS mrch_resp_startdt
	,trax_fact.visit_date AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'What is the hand-eye of the product?' AS questiontext
	,-- Changed from NULL
	NULL AS valuekey
	,NULL AS value
	,NULL AS productid
	,NULL AS mustcarryitem
	,NULL AS answerscore
	,NULL AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'PLANOGRAM COMPLIANCE' AS kpi
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS category
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS segment
	,NULL AS vst_visitid
	,trax_fact.visit_date AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,date_part(year, trax_fact.visit_date::TIMESTAMP without TIME zone) AS fisc_yr
	,to_char(trax_fact.visit_date::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed the "DD" in the format
	CASE 
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU INDY PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BEAUTY'::CHARACTER VARYING::TEXT
				)
			THEN 'IMS'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND upper(kpi_wgt.retail_environment::TEXT) = 'INDEPENDENT GROCERY'::CHARACTER VARYING::TEXT
			THEN 'METCASH'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BIG BOX'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'NZ MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'DISCOUNTER'::CHARACTER VARYING::TEXT
				)
			THEN 'IRI'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,trax_fact.storeid AS cust_remotekey
	,acct_dim.acct_display_name AS customername
	,kpi_wgt.ctry AS country
	,acct_dim.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,acct_dim.acct_banner AS storereference
	,
	/*CASE
           WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'AU INDY PHARMACY'::character varying
           ELSE trax_fact.re
       END AS storetype, 
       CASE
           WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'Pharmacy'::character varying
           WHEN upper(trax_fact.re::text) = 'AU MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'INDEPENDENT GROCERY'::character varying::text OR upper(trax_fact.re::text) = 'NZ MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'DISCOUNTER'::character varying::text THEN 'Grocery'::character varying
           WHEN upper(trax_fact.re::text) = 'AU INDY PHARMACY'::character varying::text OR upper(trax_fact.re::text) = 'BEAUTY'::character varying::text OR upper(trax_fact.re::text) = 'BIG BOX'::character varying::text THEN 'Pharmacy'::character varying
           ELSE NULL::character varying
       END AS channel, */
	CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
			THEN 'AU INDY PHARMACY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		ELSE trax_fact.re
		END AS storetype
	,CASE 
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
			THEN 'Pharmacy'
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
			THEN 'Grocery'
		ELSE acct_dim.acct_type
		END AS "channel"
	,CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'MY CHEMIST'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'Chemist Warehouse & My Chemist'::CHARACTER VARYING
		ELSE acct_dim.acct_banner
		END AS salesgroup
	,NULL AS soldtoparty
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS brand
	,trax_product.product_local_name AS productname
	,ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS eannumber
	,NULL AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,upper(trax_product.category_name::TEXT)::CHARACTER VARYING AS prod_hier_l3
	,upper(trax_product.subcategory_local_name::TEXT)::CHARACTER VARYING AS prod_hier_l4
	,upper(trax_product.brand_name::TEXT)::CHARACTER VARYING AS prod_hier_l5
	,-- Changed from NULL
	NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)::CHARACTER VARYING || ' - ' || trax_product.product_local_name AS prod_hier_l9
	,-- Changed from NULL
	kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,NULL AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,trax_fact.jj_he AS actual
	,trax_fact.all_he AS target
	,gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,COALESCE(CASE 
			WHEN to_char(to_date(trax_fact.visit_date), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
				OR to_char(to_date(trax_fact.visit_date), 'YYYYMM') < '202303'
				THEN acct_dim.store_grade
			ELSE ahd.store_grade
			END, 'Not Assigned') AS store_grade
--MAX(kpi_chnl_wt) AS "weight" -- Why is this needed? There is already the kpi_chnl_wt
FROM edw_vw_perfect_store_trax_fact trax_fact -- Transactions
LEFT JOIN (
	SELECT acct_dim.acct_id::CHARACTER VARYING AS acct_id
		,acct_dim.acct_display_name
		,acct_dim.acct_type_desc
		,acct_dim.acct_street_1
		,acct_dim.acct_street_2
		,acct_dim.acct_street_3
		,acct_dim.acct_suburb
		,acct_dim.acct_postcode
		,acct_dim.acct_phone_number
		,acct_dim.acct_fax_number
		,acct_dim.acct_email
		,acct_dim.acct_country
		,acct_dim.acct_region
		,acct_dim.acct_state
		,acct_dim.acct_banner_country
		,acct_dim.acct_banner_division
		,acct_dim.acct_banner_type
		,acct_dim.acct_banner
		,acct_dim.acct_type
		,acct_dim.acct_sub_type
		,acct_dim.acct_grade
		,acct_dim.acct_nz_pharma_country
		,acct_dim.acct_nz_pharma_state
		,acct_dim.acct_nz_pharma_territory
		,acct_dim.acct_nz_groc_country
		,acct_dim.acct_nz_groc_state
		,acct_dim.acct_nz_groc_territory
		,acct_dim.acct_ssr_country
		,acct_dim.acct_ssr_state
		,acct_dim.acct_ssr_team_leader
		,acct_dim.acct_ssr_territory
		,acct_dim.acct_ssr_sub_territory
		,acct_dim.acct_ind_groc_country
		,acct_dim.acct_ind_groc_state
		,acct_dim.acct_ind_groc_territory
		,acct_dim.acct_ind_groc_sub_territory
		,acct_dim.acct_au_pharma_country
		,acct_dim.acct_au_pharma_state
		,acct_dim.acct_au_pharma_territory
		,acct_dim.acct_au_pharma_ssr_country
		,acct_dim.acct_au_pharma_ssr_state
		,acct_dim.acct_au_pharma_ssr_territory
		,acct_dim.acct_store_code
		,acct_dim.acct_fax_opt_out
		,acct_dim.acct_email_opt_out
		,acct_dim.acct_contact_method
		,CASE 
			WHEN upper(acct_dim.acct_ind_groc_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_ind_groc_territory
			WHEN upper(acct_dim.acct_au_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_au_pharma_territory
			WHEN upper(acct_dim.acct_nz_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_nz_pharma_territory
			ELSE 'NOT ASSIGNED'::CHARACTER VARYING
			END AS acct_terriroty
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade IS NULL
				OR acct_grade = ''
				OR upper(acct_grade) = 'NOT ASSIGNED'
				THEN 'Not Assigned'
			ELSE 'D'
			END AS store_grade
	FROM edw_perenso_account_dim acct_dim
	) acct_dim -- Store Master
	ON trax_fact.storeid::TEXT = acct_dim.acct_id::TEXT
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON acct_dim.acct_id = ahd.acct_id
	AND to_char(trax_fact.visit_date, 'YYYYMM') = ahd.snapshot_mnth
LEFT JOIN (
	SELECT derived_table6.ean_number
		,derived_table6.product_local_name
		,derived_table6.brand_name
		,derived_table6.category_name
		,derived_table6.subcategory_local_name
	FROM (
		SELECT edw_vw_perfect_store_trax_products.data_source
			,edw_vw_perfect_store_trax_products.ean_number
			,edw_vw_perfect_store_trax_products.product_name
			,edw_vw_perfect_store_trax_products.product_local_name
			,edw_vw_perfect_store_trax_products.product_short_name
			,edw_vw_perfect_store_trax_products.brand_name
			,edw_vw_perfect_store_trax_products.brand_local_name
			,edw_vw_perfect_store_trax_products.manufacturer_name
			,edw_vw_perfect_store_trax_products.manufacturer_local_name
			,edw_vw_perfect_store_trax_products.size
			,edw_vw_perfect_store_trax_products.unit_measurement
			,edw_vw_perfect_store_trax_products.category_name
			,edw_vw_perfect_store_trax_products.category_local_name
			,edw_vw_perfect_store_trax_products.subcategory_local_name
			,row_number() OVER (
				PARTITION BY edw_vw_perfect_store_trax_products.ean_number ORDER BY edw_vw_perfect_store_trax_products.ean_number
				) AS row_num
		FROM edw_vw_perfect_store_trax_products
		) derived_table6
	WHERE derived_table6.row_num = 1
	) trax_product -- Product Master
	ON ltrim(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT) = ltrim(trax_product.ean_number::TEXT, 0::CHARACTER VARYING::TEXT)
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- KPI Weights
	ON kpi_wgt.kpi_name::TEXT = 'Planogram Compliance'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(kpi_wgt.retail_environment::TEXT)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Targets by RE
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'PLANOGRAM COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(mkt_share_re.re_customer_value::TEXT)
	AND upper(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_re.sub_category::TEXT)
	AND upper(kpi_wgt.ctry::TEXT) = upper(mkt_share_re.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_re.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_re.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Targets by Customer
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'PLANOGRAM COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(acct_dim.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT)
	AND upper(trax_product.subcategory_local_name::TEXT) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_cust.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_cust.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_cust.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_cust.valid_to)
LEFT JOIN (
	SELECT a.ctry_nm
		,a.ean_upc
		,gcph_category
		,gcph_subcategory
	FROM (
		-- Get GCPH by EAN by latest date
		SELECT ctry_nm
			,gcph_category
			,gcph_subcategory
			,ean_upc
			,lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
		GROUP BY 1
			,2
			,3
			,4
			,5
		) a
	JOIN (
		SELECT ctry_nm
			,ean_upc
			,lst_nts AS "latest_nts_date"
			,ROW_NUMBER() OVER (
				PARTITION BY ctry_nm
				,ean_upc ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd = 'FERT'
			AND UPPER(ctry_nm) IN (
				'AUSTRALIA'
				,'NEW ZEALAND'
				)
			AND ean_upc IS NOT NULL
			AND ean_upc <> ''
		GROUP BY 1
			,2
			,3
		) b ON a.ctry_nm = b.ctry_nm
		AND a.ean_upc = b.ean_upc
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1
		,2
		,3
		,4
	) c -- Product Key Attribute (To assign a GCPH Catgory & Sub-category to every EAN Number)
	ON c.ean_upc = LTRIM(trax_fact.productid::TEXT, 0::CHARACTER VARYING::TEXT)
	AND upper(c.ctry_nm) = upper(kpi_wgt.ctry)
WHERE trax_fact.all_he > 0::DOUBLE PRECISION
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union8 as(
SELECT 'Trax API' AS dataset
	,-- Changed from 'SURVEY_RESPONSE'
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,trax_fact.storeid AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,trax_fact.visit_date AS mrch_resp_startdt
	,trax_fact.visit_date AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'What is the J&J facing?' AS questiontext
	,-- Changed from NULL
	NULL AS valuekey
	,trax_fact.jj_shelf::CHARACTER VARYING AS value
	,-- Ask Mouna if she can use "actual" column instead. If yes, please change to NULL
	NULL AS productid
	,NULL AS mustcarryitem
	,NULL AS answerscore
	,NULL AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'SHARE OF SHELF' AS kpi
	,trax_fact.category::CHARACTER VARYING AS category
	,trax_fact.subcategory::CHARACTER VARYING AS segment
	,NULL AS vst_visitid
	,trax_fact.visit_date AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,date_part(year, trax_fact.visit_date::TIMESTAMP without TIME zone) AS fisc_yr
	,to_char(trax_fact.visit_date::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed "DD" from format
	CASE 
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU INDY PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BEAUTY'::CHARACTER VARYING::TEXT
				)
			THEN 'IMS'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND upper(kpi_wgt.retail_environment::TEXT) = 'INDEPENDENT GROCERY'::CHARACTER VARYING::TEXT
			THEN 'METCASH'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BIG BOX'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'NZ MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'DISCOUNTER'::CHARACTER VARYING::TEXT
				)
			THEN 'IRI'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,trax_fact.storeid AS cust_remotekey
	,acct_dim.acct_display_name AS customername
	,kpi_wgt.ctry AS country
	,acct_dim.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,acct_dim.acct_banner AS storereference
	,
	/*CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'AU INDY PHARMACY'::character varying
            ELSE trax_fact.re
        END AS storetype, 
        CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'Pharmacy'::character varying
            WHEN upper(trax_fact.re::text) = 'AU MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'INDEPENDENT GROCERY'::character varying::text OR upper(trax_fact.re::text) = 'NZ MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'DISCOUNTER'::character varying::text THEN 'Grocery'::character varying
            WHEN upper(trax_fact.re::text) = 'AU INDY PHARMACY'::character varying::text OR upper(trax_fact.re::text) = 'BEAUTY'::character varying::text OR upper(trax_fact.re::text) = 'BIG BOX'::character varying::text THEN 'Pharmacy'::character varying
            ELSE NULL::character varying
        END AS channel, */
	CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
			THEN 'AU INDY PHARMACY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		ELSE trax_fact.re
		END AS storetype
	,CASE 
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
			THEN 'Pharmacy'
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
			THEN 'Grocery'
		ELSE acct_dim.acct_type
		END AS "channel"
	,CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'MY CHEMIST'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'Chemist Warehouse & My Chemist'::CHARACTER VARYING
		ELSE acct_dim.acct_banner
		END AS salesgroup
	,NULL AS soldtoparty
	,NULL AS brand
	,NULL AS productname
	,NULL AS eannumber
	,NULL AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,trax_fact.category::CHARACTER VARYING AS prod_hier_l3
	,trax_fact.subcategory::CHARACTER VARYING AS prod_hier_l4
	,NULL AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,NULL AS prod_hier_l9
	,kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,'NUMERATOR' AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,trax_fact.jj_shelf::DOUBLE PRECISION AS actual
	,-- Changed from NULL
	trax_fact.jj_shelf::DOUBLE PRECISION AS target
	,gcph_category AS gcph_category
	,gcph_subcategory AS gcph_subcategory
	,COALESCE(CASE 
			WHEN to_char(to_date(trax_fact.visit_date), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
				OR to_char(to_date(trax_fact.visit_date), 'YYYYMM') < '202303'
				THEN acct_dim.store_grade
			ELSE ahd.store_grade
			END, 'Not Assigned') AS store_grade
--MAX(kpi_wgt.value) AS "weight" -- Is this needed?
FROM (
	SELECT DISTINCT a.storeid
		,a.visit_date
		,a.re
		,upper(a.category::TEXT) AS category
		,gcph_category
		,upper(a.subcategory::TEXT) AS subcategory
		,gcph_subcategory
		,sum(a.jj_shelf) OVER (
			PARTITION BY a.storeid
			,a.visit_date
			,a.re
			,a.category
			,a.subcategory order by null ROWS BETWEEN UNBOUNDED PRECEDING
				AND UNBOUNDED FOLLOWING
			) AS jj_shelf
		,"max" (a.all_shelf) OVER (
			PARTITION BY a.storeid
			,a.visit_date
			,a.re
			,a.category
			,a.subcategory order by null ROWS BETWEEN UNBOUNDED PRECEDING
				AND UNBOUNDED FOLLOWING
			) AS all_shelf
	FROM edw_vw_perfect_store_trax_fact a
	LEFT JOIN (
		SELECT a.ctry_nm
			,a.ean_upc
			,gcph_category
			,gcph_subcategory
		FROM (
			-- Get GCPH by EAN by latest date
			SELECT ctry_nm
				,gcph_category
				,gcph_subcategory
				,ean_upc
				,lst_nts AS "nts_date"
			FROM edw_product_key_attributes
			WHERE matl_type_cd = 'FERT'
			GROUP BY 1
				,2
				,3
				,4
				,5
			) a
		JOIN (
			SELECT ctry_nm
				,ean_upc
				,lst_nts AS "latest_nts_date"
				,ROW_NUMBER() OVER (
					PARTITION BY ctry_nm
					,ean_upc ORDER BY lst_nts DESC
					) AS "row_number"
			FROM edw_product_key_attributes
			WHERE matl_type_cd = 'FERT'
				AND UPPER(ctry_nm) IN (
					'AUSTRALIA'
					,'NEW ZEALAND'
					)
				AND ean_upc IS NOT NULL
				AND ean_upc <> ''
			GROUP BY 1
				,2
				,3
			) b ON a.ctry_nm = b.ctry_nm
			AND a.ean_upc = b.ean_upc
			AND "latest_nts_date" = "nts_date"
			AND "row_number" = 1
		GROUP BY 1
			,2
			,3
			,4
		) b ON a.productid = b.ean_upc
		AND CASE 
			WHEN UPPER(a.re) IN (
					'NZ MAJOR CHAIN SUPER'
					,'DISCOUNTER'
					)
				THEN 'New Zealand'
			ELSE 'Australia'
			END = b.ctry_nm
	) trax_fact -- Transaction table
LEFT JOIN (
	SELECT acct_dim.acct_id
		,acct_dim.acct_display_name
		,acct_dim.acct_type_desc
		,acct_dim.acct_street_1
		,acct_dim.acct_street_2
		,acct_dim.acct_street_3
		,acct_dim.acct_suburb
		,acct_dim.acct_postcode
		,acct_dim.acct_phone_number
		,acct_dim.acct_fax_number
		,acct_dim.acct_email
		,acct_dim.acct_country
		,acct_dim.acct_region
		,acct_dim.acct_state
		,acct_dim.acct_banner_country
		,acct_dim.acct_banner_division
		,acct_dim.acct_banner_type
		,acct_dim.acct_banner
		,acct_dim.acct_type
		,acct_dim.acct_sub_type
		,acct_dim.acct_grade
		,acct_dim.acct_nz_pharma_country
		,acct_dim.acct_nz_pharma_state
		,acct_dim.acct_nz_pharma_territory
		,acct_dim.acct_nz_groc_country
		,acct_dim.acct_nz_groc_state
		,acct_dim.acct_nz_groc_territory
		,acct_dim.acct_ssr_country
		,acct_dim.acct_ssr_state
		,acct_dim.acct_ssr_team_leader
		,acct_dim.acct_ssr_territory
		,acct_dim.acct_ssr_sub_territory
		,acct_dim.acct_ind_groc_country
		,acct_dim.acct_ind_groc_state
		,acct_dim.acct_ind_groc_territory
		,acct_dim.acct_ind_groc_sub_territory
		,acct_dim.acct_au_pharma_country
		,acct_dim.acct_au_pharma_state
		,acct_dim.acct_au_pharma_territory
		,acct_dim.acct_au_pharma_ssr_country
		,acct_dim.acct_au_pharma_ssr_state
		,acct_dim.acct_au_pharma_ssr_territory
		,acct_dim.acct_store_code
		,acct_dim.acct_fax_opt_out
		,acct_dim.acct_email_opt_out
		,acct_dim.acct_contact_method
		,CASE 
			WHEN upper(acct_dim.acct_ind_groc_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_ind_groc_territory
			WHEN upper(acct_dim.acct_au_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_au_pharma_territory
			WHEN upper(acct_dim.acct_nz_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_nz_pharma_territory
			ELSE 'NOT ASSIGNED'::CHARACTER VARYING
			END AS acct_terriroty
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade IS NULL
				OR acct_grade = ''
				OR upper(acct_grade) = 'NOT ASSIGNED'
				THEN 'Not Assigned'
			ELSE 'D'
			END AS store_grade
	FROM edw_perenso_account_dim acct_dim
	) acct_dim -- Obtain store attributes from Trax Store Master by storeid
	ON trax_fact.storeid::NUMERIC::NUMERIC(18, 0) = acct_dim.acct_id
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON acct_dim.acct_id = ahd.acct_id
	AND to_char(trax_fact.visit_date, 'YYYYMM') = ahd.snapshot_mnth
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain Share of Shelf KPI Weights by RE
	ON kpi_wgt.kpi_name::TEXT = 'SOS Compliance'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(kpi_wgt.retail_environment::TEXT)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain Share of Shelf Targets by Subcategory by RE for the relevant time period
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(mkt_share_re.re_customer_value::TEXT)
	AND upper(trax_fact.subcategory) = upper(mkt_share_re.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_re.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_re.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_re.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain Share of Shelf Targets by Subcategory by Customer for the relevant time period
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(acct_dim.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT)
	AND upper(trax_fact.subcategory) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_cust.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_cust.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_cust.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_cust.valid_to)
WHERE trax_fact.all_shelf > 0::DOUBLE PRECISION
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
union9 as(
SELECT 'Trax API' AS dataset
	,-- Changed from 'SURVEY_RESPONSE' to the "source" column in the Trax table
	NULL AS merchandisingresponseid
	,NULL AS surveyresponseid
	,trax_fact.storeid AS customerid
	,NULL AS salespersonid
	,NULL AS visitid
	,trax_fact.visit_date AS mrch_resp_startdt
	,trax_fact.visit_date AS mrch_resp_enddt
	,NULL AS mrch_resp_status
	,NULL AS mastersurveyid
	,NULL AS survey_status
	,NULL AS survey_enddate
	,NULL AS questionkey
	,'What is the total shelf facing?' AS questiontext
	,-- Changed from NULL
	NULL AS valuekey
	,trax_fact.all_shelf::CHARACTER VARYING AS value
	,-- Ask Mouna if she can use target column instead. If yes, please change to NULL
	NULL AS productid
	,NULL AS mustcarryitem
	,NULL AS answerscore
	,NULL AS presence
	,NULL AS outofstock
	,NULL AS mastersurveyname
	,'SHARE OF SHELF' AS kpi
	,trax_fact.category::CHARACTER VARYING AS category
	,-- Ask Mouna if she can use prod_hier_l3 instead. If yes, please remove
	trax_fact.subcategory::CHARACTER VARYING AS segment
	,-- Ask Mouna if she can use prod_hier_l4 instead. If yes, please remove
	NULL AS vst_visitid
	,trax_fact.visit_date AS scheduleddate
	,NULL AS scheduledtime
	,NULL AS duration
	,'COMPLETED' AS vst_status
	,date_part(year, trax_fact.visit_date::TIMESTAMP without TIME zone) AS fisc_yr
	,to_char(trax_fact.visit_date::TIMESTAMP without TIME zone, 'YYYYMM'::CHARACTER VARYING::TEXT)::CHARACTER VARYING AS fisc_per
	,-- Removed "DD" from format
	CASE 
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU INDY PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BEAUTY'::CHARACTER VARYING::TEXT
				)
			THEN 'IMS'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND upper(kpi_wgt.retail_environment::TEXT) = 'INDEPENDENT GROCERY'::CHARACTER VARYING::TEXT
			THEN 'METCASH'::CHARACTER VARYING::TEXT
		WHEN (
				upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
				)
			AND (
				upper(kpi_wgt.retail_environment::TEXT) = 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'BIG BOX'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'NZ MAJOR CHAIN SUPER'::CHARACTER VARYING::TEXT
				OR upper(kpi_wgt.retail_environment::TEXT) = 'DISCOUNTER'::CHARACTER VARYING::TEXT
				)
			THEN 'IRI'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 2)
		END::CHARACTER VARYING AS firstname
	,CASE 
		WHEN upper(acct_dim.acct_terriroty::TEXT) = 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
			OR upper(split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)) = 'VACANT'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_terriroty::TEXT) IS NULL
			THEN 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
		ELSE split_part(acct_dim.acct_terriroty::TEXT, ' '::CHARACTER VARYING::TEXT, 1)
		END::CHARACTER VARYING AS lastname
	,trax_fact.storeid AS cust_remotekey
	,acct_dim.acct_display_name AS customername
	,kpi_wgt.ctry AS country
	,acct_dim.acct_state AS STATE
	,NULL AS county
	,NULL AS district
	,NULL AS city
	,acct_dim.acct_banner AS storereference
	,
	/*CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'AU INDY PHARMACY'::character varying
            ELSE trax_fact.re
        END AS storetype, 
        CASE
            WHEN upper(acct_dim.acct_banner::text) = 'PRICELINE PHARMACY'::character varying::text OR upper(acct_dim.acct_banner::text) = 'TERRY WHITE CHEMMART'::character varying::text OR upper(acct_dim.acct_banner::text) = 'CHEMSAVE'::character varying::text THEN 'Pharmacy'::character varying
            WHEN upper(trax_fact.re::text) = 'AU MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'INDEPENDENT GROCERY'::character varying::text OR upper(trax_fact.re::text) = 'NZ MAJOR CHAIN SUPER'::character varying::text OR upper(trax_fact.re::text) = 'DISCOUNTER'::character varying::text THEN 'Grocery'::character varying
            WHEN upper(trax_fact.re::text) = 'AU INDY PHARMACY'::character varying::text OR upper(trax_fact.re::text) = 'BEAUTY'::character varying::text OR upper(trax_fact.re::text) = 'BIG BOX'::character varying::text THEN 'Pharmacy'::character varying
            ELSE NULL::character varying
        END AS channel, */
	CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
			THEN 'AU INDY PHARMACY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'BIG BOX'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
			THEN 'BEAUTY'::CHARACTER VARYING
		WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
			THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
		ELSE trax_fact.re
		END AS storetype
	,CASE 
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
			THEN 'Pharmacy'
		WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
			THEN 'Grocery'
		ELSE acct_dim.acct_type
		END AS "channel"
	,CASE 
		WHEN upper(acct_dim.acct_banner::TEXT) = 'MY CHEMIST'::CHARACTER VARYING::TEXT
			OR upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
			THEN 'Chemist Warehouse & My Chemist'::CHARACTER VARYING
		ELSE acct_dim.acct_banner
		END AS salesgroup
	,NULL AS soldtoparty
	,NULL AS brand
	,NULL AS productname
	,NULL AS eannumber
	,NULL AS matl_num
	,NULL AS prod_hier_l1
	,NULL AS prod_hier_l2
	,trax_fact.category::CHARACTER VARYING AS prod_hier_l3
	,trax_fact.subcategory::CHARACTER VARYING AS prod_hier_l4
	,NULL AS prod_hier_l5
	,NULL AS prod_hier_l6
	,NULL AS prod_hier_l7
	,NULL AS prod_hier_l8
	,NULL AS prod_hier_l9
	,kpi_wgt.value AS kpi_chnl_wt
	,CASE 
		WHEN mkt_share_cust.target IS NULL
			THEN mkt_share_re.target
		ELSE mkt_share_cust.target
		END AS mkt_share
	,'DENOMINATOR' AS ques_desc
	,NULL AS "y/n_flag"
	,NULL AS rej_reason
	,NULL AS response
	,NULL AS response_score
	,NULL AS acc_rej_reason
	,trax_fact.all_shelf::DOUBLE PRECISION AS actual
	,trax_fact.all_shelf::DOUBLE PRECISION AS target
	,gcph_category
	,gcph_subcategory
	,COALESCE(CASE 
			WHEN to_char(to_date(trax_fact.visit_date), 'YYYYMM') = to_char(current_timestamp(), 'YYYYMM')
				OR to_char(to_date(trax_fact.visit_date), 'YYYYMM') < '202303'
				THEN acct_dim.store_grade
			ELSE ahd.store_grade
			END, 'Not Assigned') AS store_grade
--MAX(kpi_wgt.value) AS "weight" -- Is this needed?
FROM (
	SELECT DISTINCT a.storeid
		,a.visit_date
		,a.re
		,upper(a.category::TEXT) AS category
		,gcph_category
		,upper(a.subcategory::TEXT) AS subcategory
		,gcph_subcategory
		,sum(a.jj_shelf) OVER (
			PARTITION BY a.storeid
			,a.visit_date
			,a.re
			,a.category
			,a.subcategory order by null ROWS BETWEEN UNBOUNDED PRECEDING
				AND UNBOUNDED FOLLOWING
			) AS jj_shelf
		,"max" (a.all_shelf) OVER (
			PARTITION BY a.storeid
			,a.visit_date
			,a.re
			,a.category
			,a.subcategory order by null ROWS BETWEEN UNBOUNDED PRECEDING
				AND UNBOUNDED FOLLOWING
			) AS all_shelf
	FROM edw_vw_perfect_store_trax_fact a
	LEFT JOIN (
		SELECT a.ctry_nm
			,a.ean_upc
			,gcph_category
			,gcph_subcategory
		FROM (
			-- Get GCPH by EAN by latest date
			SELECT ctry_nm
				,gcph_category
				,gcph_subcategory
				,ean_upc
				,lst_nts AS "nts_date"
			FROM edw_product_key_attributes
			WHERE matl_type_cd = 'FERT'
			GROUP BY 1
				,2
				,3
				,4
				,5
			) a
		JOIN (
			SELECT ctry_nm
				,ean_upc
				,lst_nts AS "latest_nts_date"
				,ROW_NUMBER() OVER (
					PARTITION BY ctry_nm
					,ean_upc ORDER BY lst_nts DESC
					) AS "row_number"
			FROM edw_product_key_attributes
			WHERE matl_type_cd = 'FERT'
				AND UPPER(ctry_nm) IN (
					'AUSTRALIA'
					,'NEW ZEALAND'
					)
				AND ean_upc IS NOT NULL
				AND ean_upc <> ''
			GROUP BY 1
				,2
				,3
			) b ON a.ctry_nm = b.ctry_nm
			AND a.ean_upc = b.ean_upc
			AND "latest_nts_date" = "nts_date"
			AND "row_number" = 1
		GROUP BY 1
			,2
			,3
			,4
		) b ON a.productid = b.ean_upc
		AND CASE 
			WHEN UPPER(a.re) IN (
					'NZ MAJOR CHAIN SUPER'
					,'DISCOUNTER'
					)
				THEN 'New Zealand'
			ELSE 'Australia'
			END = b.ctry_nm
	) trax_fact -- Transaction table
LEFT JOIN (
	SELECT acct_dim.acct_id
		,acct_dim.acct_display_name
		,acct_dim.acct_type_desc
		,acct_dim.acct_street_1
		,acct_dim.acct_street_2
		,acct_dim.acct_street_3
		,acct_dim.acct_suburb
		,acct_dim.acct_postcode
		,acct_dim.acct_phone_number
		,acct_dim.acct_fax_number
		,acct_dim.acct_email
		,acct_dim.acct_country
		,acct_dim.acct_region
		,acct_dim.acct_state
		,acct_dim.acct_banner_country
		,acct_dim.acct_banner_division
		,acct_dim.acct_banner_type
		,acct_dim.acct_banner
		,acct_dim.acct_type
		,acct_dim.acct_sub_type
		,acct_dim.acct_grade
		,acct_dim.acct_nz_pharma_country
		,acct_dim.acct_nz_pharma_state
		,acct_dim.acct_nz_pharma_territory
		,acct_dim.acct_nz_groc_country
		,acct_dim.acct_nz_groc_state
		,acct_dim.acct_nz_groc_territory
		,acct_dim.acct_ssr_country
		,acct_dim.acct_ssr_state
		,acct_dim.acct_ssr_team_leader
		,acct_dim.acct_ssr_territory
		,acct_dim.acct_ssr_sub_territory
		,acct_dim.acct_ind_groc_country
		,acct_dim.acct_ind_groc_state
		,acct_dim.acct_ind_groc_territory
		,acct_dim.acct_ind_groc_sub_territory
		,acct_dim.acct_au_pharma_country
		,acct_dim.acct_au_pharma_state
		,acct_dim.acct_au_pharma_territory
		,acct_dim.acct_au_pharma_ssr_country
		,acct_dim.acct_au_pharma_ssr_state
		,acct_dim.acct_au_pharma_ssr_territory
		,acct_dim.acct_store_code
		,acct_dim.acct_fax_opt_out
		,acct_dim.acct_email_opt_out
		,acct_dim.acct_contact_method
		,CASE 
			WHEN upper(acct_dim.acct_ind_groc_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_ind_groc_territory
			WHEN upper(acct_dim.acct_au_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_au_pharma_territory
			WHEN upper(acct_dim.acct_nz_pharma_territory::TEXT) <> 'NOT ASSIGNED'::CHARACTER VARYING::TEXT
				THEN acct_dim.acct_nz_pharma_territory
			ELSE 'NOT ASSIGNED'::CHARACTER VARYING
			END AS acct_terriroty
		,CASE 
			WHEN (
					acct_grade LIKE 'A%'
					OR acct_grade LIKE '% A %'
					)
				THEN 'A'
			WHEN (
					acct_grade LIKE 'B%'
					OR acct_grade LIKE '% B %'
					)
				THEN 'B'
			WHEN (
					acct_grade LIKE 'C%'
					OR acct_grade LIKE '% C %'
					)
				AND acct_grade NOT LIKE 'Ch%'
				THEN 'C'
			WHEN acct_grade IS NULL
				OR acct_grade = ''
				OR upper(acct_grade) = 'NOT ASSIGNED'
				THEN 'Not Assigned'
			ELSE 'D'
			END AS store_grade
	FROM edw_perenso_account_dim acct_dim
	) acct_dim -- Obtain store attributes from Trax Store Master by storeid
	ON trax_fact.storeid::NUMERIC::NUMERIC(18, 0) = acct_dim.acct_id
LEFT JOIN acct_hist_dim ahd -- taking history records for store grade
	ON acct_dim.acct_id = ahd.acct_id
	AND to_char(trax_fact.visit_date, 'YYYYMM') = ahd.snapshot_mnth
LEFT JOIN edw_pacific_perfect_store_weights kpi_wgt -- Obtain Share of Shelf Targets by Subcategory by RE for the relevant time period
	ON kpi_wgt.kpi_name::TEXT = 'SOS Compliance'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(kpi_wgt.retail_environment::TEXT)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_re -- Obtain Share of Shelf Targets by Subcategory by RE for the relevant time period
	ON upper(mkt_share_re.re_customer::TEXT) = 'RE'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_re.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(CASE 
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE PHARMACY'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'TERRY WHITE CHEMMART'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'CHEMSAVE'::CHARACTER VARYING::TEXT
				OR upper(acct_dim.acct_banner::TEXT) = 'PHARMACY ALLIANCE GROUP'::CHARACTER VARYING::TEXT
				THEN 'AU INDY PHARMACY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'CHEMIST WAREHOUSE'::CHARACTER VARYING::TEXT
				THEN 'BIG BOX'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'PRICELINE CORPORATE'::CHARACTER VARYING::TEXT
				THEN 'BEAUTY'::CHARACTER VARYING
			WHEN upper(acct_dim.acct_banner::TEXT) = 'WOOLWORTHS'::CHARACTER VARYING::TEXT
				THEN 'AU MAJOR CHAIN SUPER'::CHARACTER VARYING
			ELSE trax_fact.re
			END) = upper(mkt_share_re.re_customer_value::TEXT)
	AND upper(trax_fact.subcategory) = upper(mkt_share_re.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_re.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_re.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_re.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_re.valid_to)
LEFT JOIN edw_pacific_perfect_store_targets mkt_share_cust -- Obtain Share of Shelf Targets by Subcategory by Customer for the relevant time period
	ON upper(mkt_share_cust.re_customer::TEXT) = 'CUSTOMER'::CHARACTER VARYING::TEXT
	AND upper(mkt_share_cust.kpi::TEXT) = 'SOS COMPLIANCE'::CHARACTER VARYING::TEXT
	AND upper(acct_dim.acct_banner::TEXT) = upper(mkt_share_cust.re_customer_value::TEXT)
	AND upper(trax_fact.subcategory) = upper(mkt_share_cust.sub_category::TEXT)
	AND upper(kpi_wgt.ctry) = upper(mkt_share_cust.market::TEXT)
	AND upper(CASE 
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%PHARMACY%'
				THEN 'Pharmacy'
			WHEN UPPER(acct_dim.acct_type_desc) LIKE '%GROCERY%'
				THEN 'Grocery'
			ELSE acct_dim.acct_type
			END) = upper(mkt_share_cust.channel)
	AND trax_fact.visit_date >= to_date(mkt_share_cust.valid_from)
	AND trax_fact.visit_date <= to_date(mkt_share_cust.valid_to)
WHERE trax_fact.all_shelf > 0::DOUBLE PRECISION
GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72
),
final as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
    union all
    select * from union4
    union all
    select * from union5
    union all
    select * from union6
    union all
    select * from union7
    union all
    select * from union8
    union all
    select * from union9
)
select * from final