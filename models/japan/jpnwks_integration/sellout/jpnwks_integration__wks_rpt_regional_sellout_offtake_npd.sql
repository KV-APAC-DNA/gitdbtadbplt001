with 
itg_mds_jp_c360_eng_translation as (
	select * from {{ ref('jpnitg_integration__itg_mds_jp_c360_eng_translation') }}
),
itg_mds_ap_customer360_config as (
	select * from {{ ref('aspitg_integration__itg_mds_ap_customer360_config') }}
),
wks_japan_regional_sellout_npd as(
	select * from {{ ref('jpnwks_integration__wks_japan_regional_sellout_npd') }}
),
sellout_npd as(
			SELECT YEAR,
				QRTR_NO,
				MNTH_ID,
				MNTH_NO,
				CAL_DATE,
				COUNTRY_CODE,
				COUNTRY_NAME,
				DATA_SOURCE,
				SOLDTO_CODE,
				DISTRIBUTOR_CODE,
				DISTRIBUTOR_NAME,
				STORE_CODE,
				STORE_NAME,
				STORE_TYPE,
				DISTRIBUTOR_ADDITIONAL_ATTRIBUTE1,
				DISTRIBUTOR_ADDITIONAL_ATTRIBUTE2,
				DISTRIBUTOR_ADDITIONAL_ATTRIBUTE3,
				SAP_PARENT_CUSTOMER_KEY,
				SAP_PARENT_CUSTOMER_DESCRIPTION,
				SAP_CUSTOMER_CHANNEL_KEY,
				SAP_CUSTOMER_CHANNEL_DESCRIPTION,
				SAP_CUSTOMER_SUB_CHANNEL_KEY,
				SAP_SUB_CHANNEL_DESCRIPTION,
				SAP_GO_TO_MDL_KEY,
				SAP_GO_TO_MDL_DESCRIPTION,
				SAP_BANNER_KEY,
				SAP_BANNER_DESCRIPTION,
				SAP_BANNER_FORMAT_KEY,
				SAP_BANNER_FORMAT_DESCRIPTION,
				RETAIL_ENVIRONMENT,
				REGION,
				ZONE_OR_AREA,
				CUSTOMER_SEGMENT_KEY,
				CUSTOMER_SEGMENT_DESCRIPTION,
				GLOBAL_PRODUCT_FRANCHISE,
				GLOBAL_PRODUCT_BRAND,
				GLOBAL_PRODUCT_SUB_BRAND,
				GLOBAL_PRODUCT_VARIANT,
				GLOBAL_PRODUCT_SEGMENT,
				GLOBAL_PRODUCT_SUBSEGMENT,
				GLOBAL_PRODUCT_CATEGORY,
				GLOBAL_PRODUCT_SUBCATEGORY,
				GLOBAL_PUT_UP_DESCRIPTION,
				EAN,
				SKU_CODE,
				SKU_DESCRIPTION,
				--GREENLIGHT_SKU_FLAG,
				PKA_PRODUCT_KEY,
				PKA_PRODUCT_KEY_DESCRIPTION,
				--SLS_ORG,
				Customer_Product_Desc,
				FROM_CURRENCY,
				TO_CURRENCY,
				EXCHANGE_RATE,
				SELLOUT_SALES_QUANTITY,
				SELLOUT_SALES_VALUE,
				SELLOUT_SALES_VALUE_USD,
				0 AS SELLOUT_VALUE_LIST_PRICE,
				0 AS SELLOUT_VALUE_LIST_PRICE_USD,
				CUSTOMER_MIN_DATE,
				CUSTOMER_PRODUCT_MIN_DATE,
				MARKET_MIN_DATE,
				MARKET_PRODUCT_MIN_DATE,
				RN_CUS,
				RN_MKT,
				msl_product_code,
				msl_product_desc,
				retail_env,
				channel,
				crtd_dttm,
				updt_dttm
			FROM WKS_JAPAN_REGIONAL_SELLOUT_NPD
),
transformed as(
SELECT *,
	CASE 
		WHEN SELLOUT_SALES_QUANTITY <> 0
			THEN SELLOUT_SALES_VALUE / SELLOUT_SALES_QUANTITY
		ELSE 0
		END AS SELLING_PRICE,
	COUNT(CASE 
			WHEN FIRST_SCAN_FLAG_MARKET_LEVEL = 'Y'
				THEN FIRST_SCAN_FLAG_MARKET_LEVEL
			END) OVER (
		PARTITION BY COUNTRY_NAME,
		PKA_PRODUCT_KEY
		) AS CNT_MKT
FROM (
	SELECT *,
		CASE 
			WHEN FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL = 'Y'
				AND RN_CUS = 1
				THEN 'Y'
			ELSE 'N'
			END AS FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
		CASE 
			WHEN FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL = 'Y'
				AND FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL = 'Y'
				AND RN_MKT = 1
				THEN 'Y'
			ELSE 'N'
			END AS FIRST_SCAN_FLAG_MARKET_LEVEL
	FROM (
		SELECT *,
			-- CASE WHEN rn_cus=1 AND Customer_Product_Min_Date>dateadd(week,(select param_value from rg_sdl.sdl_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,Customer_Min_Date) THEN 'Y' ELSE 'N' END AS FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL,
			-- CASE WHEN rn_mkt=1 AND Market_Product_Min_Date>dateadd(week,(select param_value from rg_sdl.sdl_mds_ap_customer360_config where code='npd_buffer_weeks')::integer,Market_Min_Date) THEN 'Y' ELSE 'N' END AS FIRST_SCAN_FLAG_MARKET_LEVEL
			CASE 
				WHEN CUSTOMER_PRODUCT_MIN_DATE > DATEADD(WEEK, (
							SELECT PARAM_VALUE
							FROM ITG_MDS_AP_CUSTOMER360_CONFIG
							WHERE CODE = 'npd_buffer_weeks'
							)::INTEGER, CUSTOMER_MIN_DATE)
					THEN 'Y'
				ELSE 'N'
				END AS FIRST_SCAN_FLAG_PARENT_CUSTOMER_LEVEL_INITIAL,
			CASE 
				WHEN MARKET_PRODUCT_MIN_DATE > DATEADD(WEEK, (
							SELECT PARAM_VALUE
							FROM ITG_MDS_AP_CUSTOMER360_CONFIG
							WHERE CODE = 'npd_buffer_weeks'
							)::INTEGER, MARKET_MIN_DATE)
					THEN 'Y'
				ELSE 'N'
				END AS FIRST_SCAN_FLAG_MARKET_LEVEL_INITIAL
		FROM sellout_npd
		)
	)
)
select * from transformed