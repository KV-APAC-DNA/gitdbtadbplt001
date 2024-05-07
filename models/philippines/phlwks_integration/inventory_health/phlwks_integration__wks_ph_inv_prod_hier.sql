with edw_ph_siso_analysis as (
select * from {{ ref('phledw_integration__edw_ph_siso_analysis') }}
),
edw_vw_greenlight_skus as (
select * from {{ ref('aspedw_integration__edw_vw_greenlight_skus') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
final as (
SELECT *
FROM (
	SELECT a.*,
		--greenlight_sku_flag ,
		pka_product_key,
		pka_size_desc,
		pka_product_key_description,
		product_key,
		product_key_description,
		row_number() OVER (
			PARTITION BY SKU_CD ORDER BY SKU_CD
			) AS rnk
	FROM (
		SELECT *
		FROM (
			SELECT DISTINCT GLOBAL_PROD_FRANCHISE,
				GLOBAL_PROD_BRAND,
				GLOBAL_PROD_SUB_BRAND,
				GLOBAL_PROD_VARIANT,
				GLOBAL_PROD_SEGMENT,
				GLOBAL_PROD_SUBSEGMENT,
				GLOBAL_PROD_CATEGORY,
				GLOBAL_PROD_SUBCATEGORY,
				--GLOBAL_PUT_UP_DESC,
				nvl(NULLIF(sku, ''), 'NA') AS SKU_CD,
				SKU_DESC AS SKU_DESC
			FROM EDW_PH_SISO_ANALYSIS
			)
		WHERE sku_cd != 'NA'
		) a
	LEFT JOIN (
		SELECT MATL_NUM,
			--greenlight_sku_flag ,
			pka_product_key,
			pka_size_desc,
			pka_product_key_description,
			pka_product_key AS product_key,
			pka_product_key_description AS product_key_description
		--from edw_vw_greenlight_skus  where sls_org='2300')EMD
		FROM edw_material_dim
		) EMD ON ltrim(a.sku_cd, '0') = ltrim(EMD.MATL_NUM, '0')
	)
WHERE rnk = 1)
select * from final 