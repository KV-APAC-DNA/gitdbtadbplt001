WITH edw_rpt_sales_details AS (
	SELECT * FROM {{ ref('indedw_integration__edw_rpt_sales_details') }}
),
itg_in_mds_channel_mapping AS (
	SELECT * FROM {{ ref('inditg_integration__itg_in_mds_channel_mapping') }}
),
transformed AS (
	SELECT base.mth_mm,
		der.region_name,
		der.zone_name,
		der.territory_name,
		der.channel_name,
		der.class_desc,
		der.retailer_channel_level_3,
		der.franchise_name,
		der.brand_name,
		der.product_category_name,
		der.variant_name,
		SUM(der.achievement_nr) AS achievement_nr_all_msku
	FROM (
		SELECT mth_mm,
			TO_CHAR(TO_DATE(TO_CHAR(mth_mm), 'YYYYMM'), 'YYYY-MM-DD') AS sales_date
		FROM edw_rpt_sales_details -- v_rpt_sales_details
		WHERE mth_mm >= 202201 -- apply relevant channel_names
			AND business_channel = 'GT'
		GROUP BY 1,
			2
		) base
	LEFT JOIN (
		SELECT mth_mm,
			region_name,
			zone_name,
			territory_name,
			rd.channel_name,
			class_desc,
			COALESCE(CASE 
					WHEN cmap.retailer_channel_level_3::TEXT = ''::CHARACTER VARYING::TEXT
						THEN NULL::CHARACTER VARYING
					ELSE cmap.retailer_channel_level_3
					END, 'Unknown'::CHARACTER VARYING) AS retailer_channel_level_3,
			franchise_name,
			brand_name,
			product_category_name,
			variant_name,
			TO_CHAR(TO_DATE(TO_CHAR(mth_mm), 'YYYYMM'), 'YYYY-MM-DD') AS sales_date,
			SUM(achievement_nr) AS achievement_nr
		FROM edw_rpt_sales_details rd
		LEFT JOIN itg_in_mds_channel_mapping cmap ON cmap.channel_name::TEXT = CASE 
				WHEN CASE 
						WHEN rd.channel_name::TEXT = ''::CHARACTER VARYING::TEXT
							THEN NULL::CHARACTER VARYING
						ELSE rd.channel_name
						END IS NULL
					THEN 'Unknown'::CHARACTER VARYING
				ELSE rd.channel_name
				END::TEXT
			AND cmap.retailer_category_name::TEXT = CASE 
				WHEN CASE 
						WHEN rd.retailer_category_name::TEXT = ''::CHARACTER VARYING::TEXT
							THEN NULL::CHARACTER VARYING
						ELSE rd.retailer_category_name
						END IS NULL
					THEN 'Unknown'::CHARACTER VARYING
				ELSE rd.retailer_category_name
				END::TEXT
			AND cmap.retailer_class::TEXT = CASE 
				WHEN CASE 
						WHEN rd.class_desc::TEXT = ''::CHARACTER VARYING::TEXT
							THEN NULL::CHARACTER VARYING
						ELSE rd.class_desc
						END IS NULL
					THEN 'Unknown'::CHARACTER VARYING
				ELSE rd.class_desc
				END::TEXT --65,30,770/6530871
			AND cmap.territory_classification::TEXT = CASE 
				WHEN CASE 
						WHEN rd.territory_classification::TEXT = ''::CHARACTER VARYING::TEXT
							THEN NULL::CHARACTER VARYING
						ELSE rd.territory_classification
						END IS NULL
					THEN 'Unknown'::CHARACTER VARYING
				ELSE rd.territory_classification
				END::TEXT --21,80,025/21,79,917
		WHERE mth_mm >= 202111
			AND business_channel = 'GT'
		GROUP BY mth_mm,
			region_name,
			zone_name,
			territory_name,
			rd.channel_name,
			class_desc,
			COALESCE(CASE 
					WHEN cmap.retailer_channel_level_3::TEXT = ''::CHARACTER VARYING::TEXT
						THEN NULL::CHARACTER VARYING
					ELSE cmap.retailer_channel_level_3
					END, 'Unknown'::CHARACTER VARYING),
			franchise_name,
			brand_name,
			product_category_name,
			variant_name,
			TO_CHAR(TO_DATE(TO_CHAR(mth_mm), 'YYYYMM'), 'YYYY-MM-DD')
		) der ON DATEDIFF(month, der.sales_date, base.sales_date) < 3
		AND DATEDIFF(month, der.sales_date, base.sales_date) >= 0
	GROUP BY base.mth_mm,
		der.region_name,
		der.zone_name,
		der.territory_name,
		der.channel_name,
		der.class_desc,
		der.retailer_channel_level_3,
		der.franchise_name,
		der.brand_name,
		der.product_category_name,
		der.variant_name
	)
SELECT * FROM transformed