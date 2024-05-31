with wks_Indonesia_siso_propagate_final as(
select * from {{ ref('idnwks_integration__wks_indonesia_siso_propagate_final') }}
),
edw_vw_id_material_dim as(
select * from {{ ref('idnedw_integration__edw_vw_id_material_dim') }}
),
edw_product_dim as(
select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
edw_material_dim as(
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
edw_vw_id_customer_dim as(
select * from {{ ref('idnedw_integration__edw_vw_id_customer_dim') }}
),
EDW_DISTRIBUTOR_DIM as(
select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
siso as(
	SELECT *
	FROM wks_Indonesia_siso_propagate_final
),
egph as(
	SELECT *
	FROM (
		SELECT sap_matl_num,
			gph_prod_frnchse,
			gph_prod_brnd,
			gph_prod_sub_brnd,
			gph_prod_vrnt,
			gph_prod_sgmnt,
			gph_prod_subsgmnt,
			gph_prod_ctgry,
			gph_prod_subctgry, --gph_prod_put_up_desc,
			--EMD.greenlight_sku_flag as greenlight_sku_flag,
			EMD.pka_product_key AS pka_product_key,
			EMD.pka_product_key_description AS pka_product_key_description,
			EMD.pka_product_key AS product_key,
			EMD.pka_product_key_description AS product_key_description,
			EMD.pka_size_desc AS pka_size_desc,
			row_number() OVER (
				PARTITION BY sap_matl_num ORDER BY sap_matl_num,
					effective_to DESC
				) AS rnk
		FROM (
			SELECT DISTINCT sap_matl_num,
				GPH_PROD_FRNCHSE,
				GPH_PROD_BRND,
				GPH_PROD_SUB_BRND,
				GPH_PROD_VRNT,
				GPH_PROD_SGMNT,
				GPH_PROD_SUBSGMNT,
				GPH_PROD_CTGRY,
				GPH_PROD_SUBCTGRY,
				--GPH_PROD_PUT_UP_DESC,
				'999912' AS effective_to
			FROM edw_vw_id_material_dim
			WHERE CNTRY_KEY = 'ID'
			
			UNION ALL
			
			SELECT DISTINCT jj_sap_prod_id,
				franchise,
				brand AS brand,
				NULL AS sub_brand,
				variant3 AS variant,
				variant2 AS Segment,
				NULL AS sub_segment,
				NULL AS category,
				NULL AS sub_Category,
				--CAST(put_up AS VARCHAR) AS put_up,
				effective_to
			FROM edw_product_dim
			WHERE LTRIM(jj_sap_prod_id, 0) NOT IN (
					SELECT LTRIM(sap_matl_num, 0)
					FROM edw_vw_id_material_dim
					WHERE CNTRY_KEY = 'ID'
					)
			) product
		LEFT JOIN
			--edw_vw_greenlight_skus EMD
			edw_material_dim EMD ON ltrim(product.sap_matl_num, 0) = LTRIM(EMD.MATL_NUM, '0')
		)
	WHERE rnk = 1
),
cust_hier as(
	SELECT DSTRBTR_GRP_CD,
		SAP_PRNT_CUST_KEY,
		SAP_PRNT_CUST_DESC,
		SAP_CUST_CHNL_KEY,
		SAP_CUST_CHNL_DESC,
		SAP_CUST_SUB_CHNL_KEY,
		SAP_SUB_CHNL_DESC,
		SAP_GO_TO_MDL_KEY,
		SAP_GO_TO_MDL_DESC,
		SAP_BNR_KEY,
		SAP_BNR_DESC,
		SAP_BNR_FRMT_KEY,
		SAP_BNR_FRMT_DESC,
		RETAIL_ENV
	FROM (
		SELECT *,
			ROW_NUMBER() OVER (
				PARTITION BY DSTRBTR_GRP_CD ORDER BY SAP_PRNT_CUST_KEY DESC
				) AS RNUM
		FROM (
			SELECT DISTINCT T2.DSTRBTR_GRP_CD,
				SAP_PRNT_CUST_KEY,
				SAP_PRNT_CUST_DESC,
				SAP_CUST_CHNL_KEY,
				SAP_CUST_CHNL_DESC,
				SAP_CUST_SUB_CHNL_KEY,
				SAP_SUB_CHNL_DESC,
				SAP_GO_TO_MDL_KEY,
				SAP_GO_TO_MDL_DESC,
				SAP_BNR_KEY,
				SAP_BNR_DESC,
				SAP_BNR_FRMT_KEY,
				SAP_BNR_FRMT_DESC,
				RETAIL_ENV
			FROM (
				SELECT *
				FROM edw_vw_id_customer_dim
				WHERE SAP_CNTRY_CD = 'ID'
				) AS T1,
				EDW_DISTRIBUTOR_DIM AS T2
			WHERE LTRIM(T2.JJ_SAP_DSTRBTR_ID, '0') = LTRIM(T1.SAP_CUST_ID, '0')
			)
		)
	WHERE RNUM = 1
),
transformed as(
SELECT month,
	CASE 
		WHEN SISO.sap_parent_customer_key IS NULL
			THEN 'NA'
		WHEN SISO.sap_parent_customer_key = ''
			THEN 'NA'
		ELSE TRIM(SISO.sap_parent_customer_key)
		END AS dstrbtr_grp_cd,
	SISO.jj_sap_dstrbtr_nm || ' - ' || SISO.sap_parent_customer_key AS dstrbtr_grp_name,
	CASE 
		WHEN EGPH.GPH_PROD_BRND IS NULL
			THEN 'NA'
		WHEN EGPH.GPH_PROD_BRND = ''
			THEN 'NA'
		ELSE TRIM(EGPH.GPH_PROD_BRND)
		END AS GPH_PROD_BRND,
	CASE 
		WHEN EGPH.GPH_PROD_VRNT IS NULL
			THEN 'NA'
		WHEN EGPH.GPH_PROD_VRNT = ''
			THEN 'NA'
		ELSE TRIM(EGPH.GPH_PROD_VRNT)
		END AS GPH_PROD_VRNT,
	CASE 
		WHEN EGPH.GPH_PROD_SGMNT IS NULL
			THEN 'NA'
		WHEN EGPH.GPH_PROD_SGMNT = ''
			THEN 'NA'
		ELSE TRIM(EGPH.GPH_PROD_SGMNT)
		END AS GPH_PROD_SGMNT,
	CASE 
		WHEN EGPH.GPH_PROD_CTGRY IS NULL
			THEN 'NA'
		WHEN EGPH.GPH_PROD_CTGRY = ''
			THEN 'NA'
		ELSE TRIM(EGPH.GPH_PROD_CTGRY)
		END AS GPH_PROD_CTGRY,
	TRIM(NVL(NULLIF(EGPH.pka_size_desc, ''), 'NA')) AS pka_size_desc,
	TRIM(NVL(NULLIF(EGPH.pka_product_key, ''), 'NA')) AS pka_product_key,
	CASE 
		WHEN CUST_HIER.SAP_PRNT_CUST_KEY IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_PRNT_CUST_KEY = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_PRNT_CUST_KEY)
		END AS SAP_PRNT_CUST_KEY,
	sum(last_3months_so_value) AS last_3months_so_val,
	sum(last_6months_so_value) AS last_6months_so_val,
	sum(last_12months_so_value) AS last_12months_so_val,
	sum(last_36months_so_value) AS last_36months_so_val,
	CASE 
		WHEN COALESCE(last_36months_so_val, 0) > 0
			AND COALESCE(last_12months_so_val, 0) <= 0
			THEN 'N'
		ELSE 'Y'
		END AS healthy_inventory
FROM SISO
LEFT JOIN EGPH ON UPPER(LTRIM(SISO.matl_num, 0)) = UPPER(LTRIM(SAP_MATL_NUM, 0))
LEFT JOIN CUST_HIER ON UPPER(LTRIM(SISO.sap_parent_customer_key, '0')) = UPPER(LTRIM(CUST_HIER.DSTRBTR_GRP_CD, '0'))
GROUP BY month,
	SISO.jj_sap_dstrbtr_nm,
	SISO.sap_parent_customer_key,
	EGPH.GPH_PROD_BRND,
	EGPH.GPH_PROD_VRNT,
	EGPH.GPH_PROD_SGMNT,
	EGPH.GPH_PROD_CTGRY,
	CUST_HIER.SAP_PRNT_CUST_KEY,
	pka_product_key,
	pka_size_desc
)
select * from transformed