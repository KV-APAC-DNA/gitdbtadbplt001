with wks_ph_price as(
    select * from {{ source('phlwks_integration', 'wks_ph_price') }}
),
wks_ph_inv_cust_hier as(
    select * from {{ ref('phlwks_integration__wks_ph_inv_cust_hier') }}
),
wks_ph_inv_prod_hier as(
    select * from {{ ref('phlwks_integration__wks_ph_inv_prod_hier') }}
),
wks_ph_cur as(
    select * from {{ source('phlwks_integration', 'wks_ph_cur') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_ph_siso_propagate_final as(
    select * from {{ ref('phlwks_integration__wks_ph_siso_propagate_final') }}
),
a as(
   SELECT 'PH' AS country_key,
		f.*
	FROM wks_ph_siso_propagate_final f
	WHERE LEFT(MONTH, 4) >= (DATE_PART(YEAR, current_timestamp()) - 2)
),
time as(
    SELECT DISTINCT "year" AS YEAR,
			qrtr_no,
			mnth_id,
			mnth_no
		FROM EDW_VW_OS_TIME_DIM
),
transformed as(
 SELECT TIME.YEAR,
	CAST(TIME.QRTR_NO AS VARCHAR(14)) AS QRTR_NO,
	CAST(TIME.MNTH_ID AS VARCHAR(21)) AS MNTH_ID,
	TIME.MNTH_NO,
	CAST('Philippines' AS VARCHAR) AS country_name,
	CASE
		WHEN A.dstrbtr_grp_cd IS NULL
			THEN 'NA'
		WHEN A.dstrbtr_grp_cd = ''
			THEN 'NA'
		ELSE TRIM(A.dstrbtr_grp_cd)
		END AS DSTRBTR_GRP_CD,
	CASE
		WHEN A.dstr_cd_nm IS NULL
			THEN 'NA'
		WHEN A.dstr_cd_nm = ''
			THEN 'NA'
		ELSE TRIM(A.dstr_cd_nm)
		END AS DSTRBTR_GRP_CD_nm,
	CASE
		WHEN product.GLOBAL_PROD_FRANCHISE IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_FRANCHISE = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_FRANCHISE)
		END AS GLOBAL_PROD_FRANCHISE,
	CASE
		WHEN product.GLOBAL_PROD_BRAND IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_BRAND = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_BRAND)
		END AS GLOBAL_PROD_BRAND,
	CASE
		WHEN product.GLOBAL_PROD_SUB_BRAND IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_SUB_BRAND = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_SUB_BRAND)
		END AS GLOBAL_PROD_SUB_BRAND,
	CASE
		WHEN product.GLOBAL_PROD_VARIANT IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_VARIANT = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_VARIANT)
		END AS GLOBAL_PROD_VARIANT,
	CASE
		WHEN product.GLOBAL_PROD_SEGMENT IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_SEGMENT = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_SEGMENT)
		END AS GLOBAL_PROD_SEGMENT,
	CASE
		WHEN product.GLOBAL_PROD_SUBSEGMENT IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_SUBSEGMENT = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_SUBSEGMENT)
		END AS GLOBAL_PROD_SUBSEGMENT,
	CASE
		WHEN product.GLOBAL_PROD_CATEGORY IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_CATEGORY = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_CATEGORY)
		END AS GLOBAL_PROD_CATEGORY,
	CASE
		WHEN product.GLOBAL_PROD_SUBCATEGORY IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_SUBCATEGORY = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_SUBCATEGORY)
		END AS GLOBAL_PROD_SUBCATEGORY,
	--CASE
	--WHEN product.GLOBAL_PUT_UP_DESC IS NULL THEN 'NA'
	--WHEN product.GLOBAL_PUT_UP_DESC = '' THEN 'NA'
	--ELSE TRIM(product.GLOBAL_PUT_UP_DESC)
	--END AS GLOBAL_PUT_UP_DESC,
	CASE
		WHEN product.SKU_cd IS NULL
			THEN 'NA'
		WHEN product.SKU_cd = ''
			THEN 'NA'
		ELSE TRIM(product.SKU_cd)
		END AS SKU_CD,
	CASE
		WHEN product.SKU_DESC IS NULL
			THEN 'NA'
		WHEN product.SKU_DESC = ''
			THEN 'NA'
		ELSE TRIM(product.SKU_DESC)
		END AS SKU_DESCRIPTION,
	--TRIM(NVL(NULLIF(product.greenlight_sku_flag,''),'NA')) AS greenlight_sku_flag,
	TRIM(NVL(NULLIF(product.pka_product_key, ''), 'NA')) AS pka_product_key,
	TRIM(NVL(NULLIF(product.PKA_SIZE_DESC, ''), 'NA')) AS PKA_SIZE_DESC,
	TRIM(NVL(NULLIF(product.pka_product_key_description, ''), 'NA')) AS pka_product_key_description,
	TRIM(NVL(NULLIF(product.product_key, ''), 'NA')) AS product_key,
	TRIM(NVL(NULLIF(product.product_key_description, ''), 'NA')) AS product_key_description,
	PH_CUR.FROM_CCY,
	PH_CUR.TO_CCY,
	PH_CUR.EXCH_RATE,
	CASE
		WHEN CUST_HIER.SAP_PRNT_CUST_KEY IS NULL
			THEN 'Not Assigned'
		WHEN CUST_HIER.SAP_PRNT_CUST_KEY = ''
			THEN 'Not Assigned'
		ELSE TRIM(CUST_HIER.SAP_PRNT_CUST_KEY)
		END AS SAP_PRNT_CUST_KEY,
	CASE
		WHEN CUST_HIER.SAP_PRNT_CUST_DESC IS NULL
			THEN 'Not Assigned'
		WHEN CUST_HIER.SAP_PRNT_CUST_DESC = ''
			THEN 'Not Assigned'
		ELSE TRIM(CUST_HIER.SAP_PRNT_CUST_DESC)
		END AS SAP_PRNT_CUST_DESC,
	CASE
		WHEN CUST_HIER.SAP_CUST_CHNL_KEY IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_CUST_CHNL_KEY = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_CUST_CHNL_KEY)
		END AS SAP_CUST_CHNL_KEY,
	CASE
		WHEN CUST_HIER.SAP_CUST_CHNL_DESC IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_CUST_CHNL_DESC = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_CUST_CHNL_DESC)
		END SAP_CUST_CHNL_DESC,
	CASE
		WHEN CUST_HIER.SAP_CUST_SUB_CHNL_KEY IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_CUST_SUB_CHNL_KEY = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_CUST_SUB_CHNL_KEY)
		END AS SAP_CUST_SUB_CHNL_KEY,
	CASE
		WHEN CUST_HIER.SAP_SUB_CHNL_DESC IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_SUB_CHNL_DESC = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_SUB_CHNL_DESC)
		END AS SAP_SUB_CHNL_DESC,
	CASE
		WHEN CUST_HIER.SAP_GO_TO_MDL_KEY IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_GO_TO_MDL_KEY = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_GO_TO_MDL_KEY)
		END AS SAP_GO_TO_MDL_KEY,
	CASE
		WHEN CUST_HIER.SAP_GO_TO_MDL_DESC IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_GO_TO_MDL_DESC = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_GO_TO_MDL_DESC)
		END AS SAP_GO_TO_MDL_DESC,
	CASE
		WHEN CUST_HIER.SAP_BNR_KEY IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_BNR_KEY = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_BNR_KEY)
		END AS SAP_BNR_KEY,
	CASE
		WHEN CUST_HIER.SAP_BNR_DESC IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_BNR_DESC = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_BNR_DESC)
		END AS SAP_BNR_DESC,
	CASE
		WHEN CUST_HIER.SAP_BNR_FRMT_KEY IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_BNR_FRMT_KEY = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_BNR_FRMT_KEY)
		END AS SAP_BNR_FRMT_KEY,
	CASE
		WHEN CUST_HIER.SAP_BNR_FRMT_DESC IS NULL
			THEN 'NA'
		WHEN CUST_HIER.SAP_BNR_FRMT_DESC = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.SAP_BNR_FRMT_DESC)
		END AS SAP_BNR_FRMT_DESC,
	CASE
		WHEN CUST_HIER.RETAIL_ENV IS NULL
			THEN 'NA'
		WHEN CUST_HIER.RETAIL_ENV = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.RETAIL_ENV)
		END AS RETAIL_ENV,
	CASE
		--                WHEN CUST_HIER.REGION IS NULL THEN 'NA'
		--                WHEN CUST_HIER.REGION = '' THEN 'NA'
		--                ELSE TRIM(CUST_HIER.REGION)
		WHEN a.sls_grp_desc IS NULL
			THEN 'NA'
		WHEN a.sls_grp_desc = ''
			THEN 'NA'
		ELSE TRIM(a.sls_grp_desc)
		END AS REGION,
	CASE
		WHEN CUST_HIER.ZONE_OR_AREA IS NULL
			THEN 'NA'
		WHEN CUST_HIER.ZONE_OR_AREA = ''
			THEN 'NA'
		ELSE TRIM(CUST_HIER.ZONE_OR_AREA)
		END AS ZONE_OR_AREA,
	SUM(last_3months_so) AS last_3months_so_qty,
	SUM(last_6months_so) AS last_6months_so_qty,
	SUM(last_12months_so) AS last_12months_so_qty,
	SUM(last_3months_so_value) AS last_3months_so_val,
	SUM(last_6months_so_value) AS last_6months_so_val,
	SUM(last_12months_so_value) AS last_12months_so_val,
	SUM(last_36months_so_value) AS last_36months_so_val,
	CAST((SUM(last_3months_so_value) * PH_CUR.Exch_rate) AS NUMERIC(38, 5)) AS last_3months_so_val_usd,
	CAST((SUM(last_6months_so_value) * PH_CUR.Exch_rate) AS NUMERIC(38, 5)) AS last_6months_so_val_usd,
	CAST((SUM(last_12months_so_value) * PH_CUR.Exch_rate) AS NUMERIC(38, 5)) AS last_12months_so_val_usd,
	propagate_flag,
	propagate_from,
	CASE
		WHEN propagate_flag = 'N'
			THEN 'Not propagate'
		ELSE reason
		END AS reason,
	replicated_flag,
	SUM(A.sell_in_qty) AS SI_SLS_QTY,
	SUM(A.sell_in_value) AS SI_GTS_VAL,
	SUM(A.sell_in_value * EXCH_RATE) AS SI_GTS_VAL_USD,
	SUM(inv_qty) AS INVENTORY_QUANTITY,
	SUM(inv_value) AS INVENTORY_VAL,
	SUM(inv_value * PH_CUR.EXCH_RATE) AS INVENTORY_VAL_USD,
	SUM(A.so_qty) AS SO_SLS_QTY,
	SUM(A.so_value) AS SO_GRS_TRD_SLS,
	ROUND(SUM((so_value * PH_CUR.EXCH_RATE))) AS SO_GRS_TRD_SLS_USD
    FROM A,
	wks_ph_cur ph_cur,
	wks_ph_inv_prod_hier AS Product,
	wks_ph_inv_cust_hier AS CUST_HIER,
	TIME,
	wks_ph_price AS LP
    WHERE a.matl_num = product.sku_cd(+)
	AND a.sap_parent_customer_key = CUST_HIER.sap_prnt_cust_key(+)
	AND a.parent_customer_cd = CUST_HIER.parent_cust_cd(+)
	AND a.month = TIME.mnth_id
	AND CAST(LP.JJ_MNTH_ID(+) AS INT) = CAST(A.month AS INT)
	AND LTRIM(LP.ITEM_CD(+), '0') = LTRIM(A.matl_num, '0')
	AND a.country_key = ph_cur.cntry_key
    GROUP BY TIME.YEAR,
        TIME.QRTR_NO,
        TIME.MNTH_ID,
        TIME.MNTH_NO,
        CNTRY_NM,
        a.dstrbtr_grp_cd,
        a.dstr_cd_nm,
        product.GLOBAL_PROD_FRANCHISE,
        product.GLOBAL_PROD_BRAND,
        product.GLOBAL_PROD_SUB_BRAND,
        product.GLOBAL_PROD_VARIANT,
        product.GLOBAL_PROD_SEGMENT,
        product.GLOBAL_PROD_SUBSEGMENT,
        product.GLOBAL_PROD_CATEGORY,
        product.GLOBAL_PROD_SUBCATEGORY,
       -- product.GLOBAL_PUT_UP_DESC,
        product.SKU_cd,
        product.SKU_DESC,
        --product.greenlight_sku_flag,
        product.pka_product_key,
        product.PKA_SIZE_DESC,
        product.pka_product_key_description,
        product.product_key,
        product.product_key_description,
        PH_CUR.FROM_CCY,
        PH_CUR.TO_CCY,
        PH_CUR.EXCH_RATE,
        CUST_HIER.SAP_PRNT_CUST_KEY,
        CUST_HIER.SAP_PRNT_CUST_DESC,
        CUST_HIER.SAP_CUST_CHNL_KEY,
        CUST_HIER.SAP_CUST_CHNL_DESC,
        CUST_HIER.SAP_CUST_SUB_CHNL_KEY,
        CUST_HIER.SAP_SUB_CHNL_DESC,
        CUST_HIER.SAP_GO_TO_MDL_KEY,
        CUST_HIER.SAP_GO_TO_MDL_DESC,
        CUST_HIER.SAP_BNR_KEY,
        CUST_HIER.SAP_BNR_DESC,
        CUST_HIER.SAP_BNR_FRMT_KEY,
        CUST_HIER.SAP_BNR_FRMT_DESC,
        CUST_HIER.RETAIL_ENV,
        a.sls_grp_desc,
        CUST_HIER.ZONE_OR_AREA,
        propagate_flag,
        propagate_from,
        reason,
        replicated_flag
)
select * from transformed