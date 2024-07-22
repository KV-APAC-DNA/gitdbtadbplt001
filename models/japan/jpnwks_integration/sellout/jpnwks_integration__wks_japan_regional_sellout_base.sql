with DW_SO_SELL_OUT_DLY as(
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.DW_SO_SELL_OUT_DLY
),
VW_JAN_CHANGE as(
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.VW_JAN_CHANGE
),
EDW_VW_OS_TIME_DIM as(
	select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
),
EDI_CHN_M as (
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.EDI_CHN_M
),
MT_SGMT as (
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.MT_SGMT
),
EDI_CSTM_M as (
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.EDI_CSTM_M
),
itg_mds_jp_c360_eng_translation as (
	select * from DEV_DNA_CORE.SNAPJPNITG_INTEGRATION.ITG_MDS_JP_C360_ENG_TRANSLATION
),
itg_mds_ap_customer360_config as (
	select * from DEV_DNA_CORE.SNAPASPITG_INTEGRATION.ITG_MDS_AP_CUSTOMER360_CONFIG
),
EDI_STORE_M as (
	select * from DEV_DNA_CORE.SNAPJPNEDW_INTEGRATION.EDI_STORE_M
),
base as(
		SELECT 'JP' AS CNTRY_CD,
		'Japan' AS CNTRY_NM,
		'SELL-OUT' AS DATA_SRC,
		CHN_HQ.LGL_NM AS DISTRIBUTOR_CODE,
		SGMT.SGMT_NM_REP,
		MDS.NAME,
		MDS1.NAME,
		MDS.NAME_ENG,
		MDS1.NAME_ENG,
		MDS.NAME_JP,
		MDS1.NAME_JP,
		CASE 
			WHEN MDS.NAME = 'Retailer Name'
				THEN MDS.NAME_ENG
			WHEN MDS1.NAME = 'Store Type'
				AND MDS1.NAME_ENG <> 'Others'
				THEN ('Other' || ' ' || MDS1.NAME_ENG)
			WHEN MDS1.NAME = 'Store Type'
				AND MDS1.NAME_ENG = 'Others'
				THEN MDS1.NAME_ENG
			ELSE 'NA'
			END AS DISTRIBUTOR_NAME,
		CASE 
			WHEN MDS1.NAME = 'Store Type'
				THEN MDS1.NAME_ENG
			END AS STORE_TYPE_CODE,
		SELLOUT.JCP_STR_CD AS STORE_CODE,
		STORE.CMMN_NM_KNJ AS STORE_NAME,
		--ECSD.PRNT_CUST_KEY AS SAP_PRNT_CUST_KEY,
		JCP_SHP_TO_CD AS SOLD_TO_CODE,
		ITEM.ITEM_CD AS SKU_CD,
		SELLOUT.ITEM_CD AS EAN,
		B."year" AS YEAR,
		B.MNTH_ID,
		SELLOUT.SHP_DATE AS DAY,
		SELLOUT.item_nm AS Customer_Product_Desc,
		SUM(SELLOUT.QTY) AS SO_SLS_QTY,
		SUM(SELLOUT.JCP_NET_PRICE) AS SO_SLS_VALUE,
		ITEM.ITEM_CD AS msl_product_code,
		SELLOUT.item_nm AS msl_product_desc,
		CASE --WHEN MDS.NAME = 'Retailer Name' THEN MDS.NAME_ENG
			WHEN MDS1.NAME = 'Store Type'
				AND MDS1.NAME_ENG <> 'Others'
				THEN ('Other' || ' ' || MDS1.NAME_ENG)
			WHEN MDS1.NAME = 'Store Type'
				AND MDS1.NAME_ENG = 'Others'
				THEN MDS1.NAME_ENG
			ELSE 'NA'
			END AS retail_env,
		SGMT.SGMT AS channel
	FROM DW_SO_SELL_OUT_DLY SELLOUT
	LEFT JOIN VW_JAN_CHANGE ITEM ON SELLOUT.ITEM_CD = ITEM.JAN_CD
	LEFT JOIN EDW_VW_OS_TIME_DIM B ON to_date(SELLOUT.SHP_DATE) = B.CAL_DATE
	--LEFT JOIN EDI_STORE_M STORE ON SELLOUT.JCP_STR_CD = STORE.STR_CD
	LEFT JOIN (
		SELECT *
		FROM (
			SELECT *,
				ROW_NUMBER() OVER (
					PARTITION BY STR_CD ORDER BY create_dt DESC
					) RN
			FROM EDI_STORE_M
			)
		WHERE RN = 1
		) STORE ON SELLOUT.JCP_STR_CD = STORE.STR_CD
	LEFT JOIN EDI_CHN_M CHN ON STORE.CHN_CD = CHN.CHN_CD
	LEFT JOIN EDI_CHN_M CHN_HQ ON CHN_HQ.CHN_CD = CHN.CHN_OFFC_CD
	LEFT JOIN MT_SGMT SGMT ON CHN.SGMT = SGMT.SGMT
	LEFT JOIN EDI_CSTM_M CSTM ON CSTM.CSTM_CD = SELLOUT.JCP_SHP_TO_CD
	LEFT JOIN itg_mds_jp_c360_eng_translation MDS ON (
			TRIM(CHN_HQ.LGL_NM) = TRIM(MDS.NAME_JP)
			AND MDS.NAME = 'Retailer Name'
			)
	LEFT JOIN itg_mds_jp_c360_eng_translation MDS1 ON (
			TRIM(SGMT.SGMT_NM_REP) = TRIM(MDS1.NAME_JP)
			AND MDS1.NAME = 'Store Type'
			)
	-- LEFT JOIN (SELECT DISTINCT CUST_NUM,REC_CRT_DT,PRNT_CUST_KEY,ROW_NUMBER() OVER (PARTITION BY CUST_NUM ORDER BY REC_CRT_DT DESC)RN from RG_EDW.EDW_CUSTOMER_SALES_DIM) 
	-- ECSD on LTRIM(CUST_NUM,'0')=SELLOUT.JCP_SHP_TO_CD WHERE RN=1
	GROUP BY CHN_HQ.LGL_NM,
		SGMT.SGMT_NM_REP,
		MDS.NAME,
		MDS1.NAME,
		MDS.NAME_ENG,
		MDS1.NAME_ENG,
		MDS.NAME_JP,
		MDS1.NAME_JP,
		DISTRIBUTOR_NAME,
		STORE_TYPE_CODE,
		SELLOUT.JCP_STR_CD,
		STORE.CMMN_NM_KNJ,
		--ECSD.PRNT_CUST_KEY,
		JCP_SHP_TO_CD,
		ITEM.ITEM_CD,
		SELLOUT.ITEM_CD,
		SELLOUT.item_nm,
		b."year",
		B.MNTH_ID,
		SELLOUT.SHP_DATE,
		SGMT.SGMT
),
transformed as(
SELECT BASE.cntry_cd,
	BASE.cntry_nm,
	BASE.data_src,
	BASE.distributor_code,
	BASE.distributor_name,
	BASE.store_code,
	BASE.store_name,
	BASE.STORE_TYPE_CODE
	--,BASE.sap_prnt_cust_key
	,
	BASE.sold_to_code,
	BASE.sku_cd,
	BASE.EAN,
	BASE.year,
	BASE.mnth_id,
	BASE.day,
	BASE.Customer_Product_Desc,
	BASE.so_sls_qty,
	BASE.so_sls_value,
	BASE.msl_product_code,
	BASE.msl_product_desc,
	UPPER(BASE.retail_env) AS retail_env,
	UPPER(BASE.channel) AS channel,
	current_timestamp()::timestamp_ntz(9) AS crtd_dttm,
	current_timestamp()::timestamp_ntz(9) AS updt_dttm
FROM BASE
WHERE NOT (
		nvl(BASE.so_sls_value, 0) = 0
		AND nvl(BASE.so_sls_qty, 0) = 0
		)
	AND BASE.day > (
		SELECT to_date(param_value, 'YYYY-MM-DD')
		FROM itg_mds_ap_customer360_config
		WHERE code = 'min_date'
		)
	AND BASE.mnth_id >= (
		CASE 
			WHEN (
					SELECT param_value
					FROM itg_mds_ap_customer360_config
					WHERE code = 'base_load_jp'
					) = 'ALL'
				THEN '190001'
			ELSE to_char(add_months(CURRENT_DATE, - (
							(
								SELECT param_value
								FROM itg_mds_ap_customer360_config
								WHERE code = 'base_load_jp'
								)::INTEGER
							)), 'YYYYMM')
			END
		)
)
select * from transformed

