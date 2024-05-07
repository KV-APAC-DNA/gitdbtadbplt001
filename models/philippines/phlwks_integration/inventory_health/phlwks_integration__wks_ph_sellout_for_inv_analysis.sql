with edw_vw_os_sellout_sales_fact as (
select * from {{ ref('phledw_integration__edw_vw_ph_sellout_sales_fact') }}
),
edw_vw_os_dstrbtr_customer_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_customer_dim') }}
),
edw_vw_os_dstrbtr_material_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_dstrbtr_material_dim') }}
),
itg_mds_ph_lav_product as (
select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
itg_mds_ph_ref_distributors as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ref_distributors') }}
),
edw_mv_ph_customer_dim as (
select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
edw_vw_os_customer_dim as (
select * from {{ ref('phledw_integration__edw_vw_ph_customer_dim') }}
),
edw_vw_ph_si_pos_inv_analysis as (
select * from {{ ref('phledw_integration__edw_vw_ph_si_pos_inv_analysis') }}
),
edw_mv_ph_customer_dim as (
select * from {{ ref('phledw_integration__edw_mv_ph_customer_dim') }}
),
wks_ph_inv_prod_hier as (
select * from {{ ref('phlwks_integration__wks_ph_inv_prod_hier') }}
),
wks_ph_inv_cust_hier as (
select * from {{ ref('phlwks_integration__wks_ph_inv_cust_hier') }}
),
final as(
SELECT min(bill_date) AS min_date,
	CASE 
		WHEN A.dstr_cd_nm IS NULL
			THEN 'NA'
		WHEN A.dstr_cd_nm = ''
			THEN 'NA'
		ELSE TRIM(A.dstr_cd_nm)
		END AS DSTRBTR_GRP_CD_nm,
	TRIM(nvl(nullif(A.dstrbtr_grp_cd, ''), 'NA')) AS dstrbtr_grp_cd,
	CASE 
		WHEN product.GLOBAL_PROD_BRAND IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_BRAND = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_BRAND)
		END AS GLOBAL_PROD_BRAND,
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
		WHEN product.GLOBAL_PROD_CATEGORY IS NULL
			THEN 'NA'
		WHEN product.GLOBAL_PROD_CATEGORY = ''
			THEN 'NA'
		ELSE TRIM(product.GLOBAL_PROD_CATEGORY)
		END AS GLOBAL_PROD_CATEGORY,
	TRIM(NVL(NULLIF(product.pka_product_key, ''), 'NA')) AS pka_product_key,
	TRIM(NVL(NULLIF(product.pka_size_desc, ''), 'NA')) AS pka_size_desc,
	CASE 
		WHEN CUST_HIER.SAP_PRNT_CUST_KEY IS NULL
			THEN 'Not Assigned'
		WHEN CUST_HIER.SAP_PRNT_CUST_KEY = ''
			THEN 'Not Assigned'
		ELSE TRIM(CUST_HIER.SAP_PRNT_CUST_KEY)
		END AS SAP_PRNT_CUST_KEY
FROM (
	SELECT nvl(nullif(b.dstrbtr_grp_cd || ' - ' || mds.dstrbtr_grp_nm, ''), 'NA') AS dstr_cd_nm,
		b.dstrbtr_grp_cd,
		b.bill_date,
		b.SO_GRS_TRD_SLS,
		nvl(nullif(b.sku, ''), 'NA') AS matl_num,
		nvl(nullif(eocd.parent_cust_cd, ''), 'NA') AS PARENT_CUSTOMER_CD,
		NVL(CUST_HIER.SAP_PRNT_CUST_KEY, eocd.parent_cust_cd) AS SAP_PRNT_CUST_KEY
	FROM (
		SELECT a.dstrbtr_grp_cd,
			a.bill_date,
			a.SO_GRS_TRD_SLS,
			ltrim(veodmd.sap_matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) AS sku,
			veodcd.sap_soldto_code
		FROM (
			SELECT dstrbtr_grp_cd,
				bill_date,
				JJ_GRS_TRD_SLS + JJ_RET_VAL AS SO_GRS_TRD_SLS,
				UPPER((dstrbtr_matl_num::TEXT)) AS dstrbtr_matl_num
			FROM edw_vw_os_sellout_sales_fact
			WHERE cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
				AND SO_GRS_TRD_SLS > 0
			) a
		LEFT JOIN (
			SELECT DISTINCT edw_vw_os_dstrbtr_customer_dim.dstrbtr_grp_cd,
				edw_vw_os_dstrbtr_customer_dim.sap_soldto_code
			FROM edw_vw_os_dstrbtr_customer_dim
			WHERE edw_vw_os_dstrbtr_customer_dim.cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
			) veodcd ON veodcd.dstrbtr_grp_cd::TEXT = a.dstrbtr_grp_cd::TEXT
		LEFT JOIN (
			SELECT edw_vw_os_dstrbtr_material_dim.dstrbtr_matl_num,
				edw_vw_os_dstrbtr_material_dim.dstrbtr_grp_cd,
				edw_vw_os_dstrbtr_material_dim.sap_matl_num
			FROM edw_vw_os_dstrbtr_material_dim
			WHERE edw_vw_os_dstrbtr_material_dim.cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
			) veodmd ON veodmd.dstrbtr_grp_cd::TEXT = a.dstrbtr_grp_cd::TEXT
			AND upper((veodmd.dstrbtr_matl_num::TEXT)) = a.dstrbtr_matl_num::TEXT
		WHERE NOT (
				LTRIM(a.dstrbtr_matl_num, '0'::CHARACTER VARYING::TEXT) IN (
					SELECT DISTINCT LTRIM(COALESCE(itg_mds_ph_lav_product.item_cd, 'NA'::CHARACTER VARYING)::TEXT, '0'::CHARACTER VARYING::TEXT) AS LTRIM
					FROM itg_mds_ph_lav_product
					WHERE itg_mds_ph_lav_product.active::TEXT = 'Y'::CHARACTER VARYING::TEXT
					)
				)
		
		UNION ALL
		
		SELECT a.dstrbtr_grp_cd,
			a.bill_date,
			a.SO_GRS_TRD_SLS,
			ltrim(a.sap_matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) AS sku,
			veodcd.sap_soldto_code
		FROM (
			SELECT dstrbtr_grp_cd,
				bill_date,
				JJ_GRS_TRD_SLS + JJ_RET_VAL AS SO_GRS_TRD_SLS,
				dstrbtr_matl_num AS sap_matl_num,
				UPPER((dstrbtr_matl_num::TEXT)) AS dstrbtr_matl_num
			FROM edw_vw_os_sellout_sales_fact
			WHERE cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
				AND SO_GRS_TRD_SLS > 0
			) a
		LEFT JOIN (
			SELECT DISTINCT edw_vw_os_dstrbtr_customer_dim.dstrbtr_grp_cd,
				edw_vw_os_dstrbtr_customer_dim.sap_soldto_code
			FROM edw_vw_os_dstrbtr_customer_dim
			WHERE edw_vw_os_dstrbtr_customer_dim.cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
			) veodcd ON veodcd.dstrbtr_grp_cd::TEXT = a.dstrbtr_grp_cd::TEXT
		WHERE NOT (
				LTRIM(a.dstrbtr_matl_num, '0'::CHARACTER VARYING::TEXT) IN (
					SELECT DISTINCT LTRIM(COALESCE(dstrbtr_matl.dstrbtr_matl_num, 'NA'::CHARACTER VARYING::TEXT), '0'::CHARACTER VARYING::TEXT) AS LTRIM
					FROM (
						SELECT a.dstrbtr_grp_cd,
							a.bill_date,
							a.SO_GRS_TRD_SLS,
							ltrim(veodmd.sap_matl_num::TEXT, '0'::CHARACTER VARYING::TEXT) AS sku,
							a.dstrbtr_matl_num,
							veodcd.sap_soldto_code
						FROM (
							SELECT dstrbtr_grp_cd,
								bill_date,
								JJ_GRS_TRD_SLS + JJ_RET_VAL AS SO_GRS_TRD_SLS,
								UPPER((dstrbtr_matl_num::TEXT)) AS dstrbtr_matl_num
							FROM edw_vw_os_sellout_sales_fact
							WHERE cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
								AND SO_GRS_TRD_SLS > 0
							) a
						LEFT JOIN (
							SELECT DISTINCT edw_vw_os_dstrbtr_customer_dim.dstrbtr_grp_cd,
								edw_vw_os_dstrbtr_customer_dim.sap_soldto_code
							FROM edw_vw_os_dstrbtr_customer_dim
							WHERE edw_vw_os_dstrbtr_customer_dim.cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
							) veodcd ON veodcd.dstrbtr_grp_cd::TEXT = a.dstrbtr_grp_cd::TEXT
						LEFT JOIN (
							SELECT edw_vw_os_dstrbtr_material_dim.dstrbtr_matl_num,
								edw_vw_os_dstrbtr_material_dim.dstrbtr_grp_cd,
								edw_vw_os_dstrbtr_material_dim.sap_matl_num
							FROM edw_vw_os_dstrbtr_material_dim
							WHERE edw_vw_os_dstrbtr_material_dim.cntry_cd::TEXT = 'PH'::CHARACTER VARYING::TEXT
							) veodmd ON veodmd.dstrbtr_grp_cd::TEXT = a.dstrbtr_grp_cd::TEXT
							AND upper((veodmd.dstrbtr_matl_num::TEXT)) = a.dstrbtr_matl_num::TEXT
						WHERE NOT (
								LTRIM(a.dstrbtr_matl_num, '0'::CHARACTER VARYING::TEXT) IN (
									SELECT DISTINCT LTRIM(COALESCE(itg_mds_ph_lav_product.item_cd, 'NA'::CHARACTER VARYING)::TEXT, '0'::CHARACTER VARYING::TEXT) AS LTRIM
									FROM itg_mds_ph_lav_product
									WHERE itg_mds_ph_lav_product.active::TEXT = 'Y'::CHARACTER VARYING::TEXT
									)
								)
						) dstrbtr_matl
					)
				)
		) b
	LEFT JOIN (
		SELECT DISTINCT primary_sold_to,
			dstrbtr_grp_cd,
			dstrbtr_grp_nm
		FROM itg_mds_ph_ref_distributors
		) mds ON b.dstrbtr_grp_cd = mds.dstrbtr_grp_cd
	LEFT JOIN edw_mv_ph_customer_dim eocd ON eocd.cust_id::TEXT = b.sap_soldto_code::TEXT
	LEFT JOIN (
		SELECT DISTINCT DSTRBTR_GRP_CD,
			SAP_SOLDTO_CODE
		FROM EDW_VW_OS_DSTRBTR_CUSTOMER_DIM
		WHERE CNTRY_CD = 'PH'
		) AS CUST_MAP ON b.DSTRBTR_GRP_CD = CUST_MAP.DSTRBTR_GRP_CD
	LEFT JOIN (
		SELECT DISTINCT T1.SAP_PRNT_CUST_KEY,
			T1.SAP_PRNT_CUST_DESC,
			T1.SAP_CUST_ID
		FROM EDW_VW_OS_CUSTOMER_DIM T1
		WHERE T1.SAP_CNTRY_CD = 'PH'
		) CUST_HIER ON LTRIM(CUST_MAP.SAP_SOLDTO_CODE, '0') = LTRIM(CUST_HIER.SAP_CUST_ID, '0')
	
	UNION ALL
	
	SELECT nvl(nullif(C.parent_cust_nm, ''), 'NA') AS sls_grp_dstr,
		C.dstrbtr_grp_cd,
		C.bill_date,
		C.SO_GRS_TRD_SLS,
		C.matl_num,
		nvl(nullif(C.parent_cust_cd, ''), 'NA') AS PARENT_CUSTOMER_CD,
		NVL(CUST_HIER.SAP_PRNT_CUST_KEY, C.PARENT_CUSTOMER_CD) AS SAP_PRNT_CUST_KEY
	FROM (
		SELECT INV.dstrbtr_grp_cd,
			INV.bill_date,
			INV.SO_GRS_TRD_SLS,
			INV.matl_num,
			INV.parent_cust_nm,
			INV.parent_cust_cd,
			INV.SOLD_TO,
			EOCD.SAP_PRNT_CUST_KEY AS PARENT_CUSTOMER_CD
		FROM (
			SELECT inv.dstrbtr_grp_cd,
				CASE 
					WHEN inv.jj_mnth_id IS NULL
						THEN NULL
					ELSE cast((left(inv.jj_mnth_id, 4) || '-' || substring(inv.jj_mnth_id, 5, 6) || '-01') AS DATE)
					END AS bill_date,
				inv.jj_gts AS SO_GRS_TRD_SLS,
				nvl(nullif(inv.sku_cd, ''), 'NA') AS matl_num,
				cust.parent_cust_nm,
				cust.parent_cust_cd,
				inv.SOLD_TO
			FROM (
				SELECT *
				FROM EDW_VW_PH_SI_POS_INV_ANALYSIS
				WHERE jj_gts > 0
					AND JJ_YEAR >= (DATE_PART(YEAR, current_timestamp()) - 6)
				) inv
			LEFT JOIN (
				SELECT DISTINCT cust_id,
					cust_nm,
					parent_cust_cd,
					parent_cust_nm,
					dstrbtr_grp_cd,
					dstrbtr_grp_nm,
					rpt_grp_2_desc
				FROM EDW_MV_PH_CUSTOMER_DIM
				) cust ON inv.sold_to = cust.cust_id
			) INV,
			(
				SELECT *
				FROM EDW_VW_OS_CUSTOMER_DIM
				WHERE SAP_CNTRY_CD = 'PH'
				) AS EOCD
		WHERE LTRIM(EOCD.SAP_CUST_ID(+), '0') = LTRIM(INV.SOLD_TO, '0')
		) C
	LEFT JOIN (
		SELECT T1.*,
			T2.REGION_NM AS REGION,
			T2.PROVINCE_NM AS ZONE_OR_AREA
		FROM EDW_VW_OS_CUSTOMER_DIM T1,
			EDW_MV_PH_CUSTOMER_DIM T2
		WHERE T1.SAP_CNTRY_CD = 'PH'
			AND LTRIM(T1.SAP_CUST_ID, '0') = LTRIM(T2.CUST_ID(+), '0')
		) CUST_HIER ON LTRIM(C.SOLD_TO, '0') = LTRIM(CUST_HIER.SAP_CUST_ID, '0')
	) A,
	wks_ph_inv_prod_hier AS product,
	wks_ph_inv_cust_hier AS CUST_HIER
WHERE A.matl_num = product.sku_cd(+)
	AND A.SAP_PRNT_CUST_KEY = CUST_HIER.sap_prnt_cust_key(+)
	AND A.parent_customer_cd = CUST_HIER.parent_cust_cd(+)
	AND left(bill_date, 4) > (DATE_PART(YEAR, current_timestamp()) - 6)
GROUP BY A.dstrbtr_grp_cd,
	A.dstr_cd_nm,
	product.GLOBAL_PROD_BRAND,
	product.GLOBAL_PROD_VARIANT,
	product.GLOBAL_PROD_SEGMENT,
	product.GLOBAL_PROD_CATEGORY,
	product.pka_product_key,
	product.pka_size_desc,
	CUST_HIER.SAP_PRNT_CUST_KEY
)
select * from final