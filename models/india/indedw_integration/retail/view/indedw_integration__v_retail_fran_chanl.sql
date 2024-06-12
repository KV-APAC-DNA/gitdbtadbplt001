with edw_retailer_dim as(
	select * from DEV_DNA_CORE.SNAPINDEDW_INTEGRATION.EDW_RETAILER_DIM
),
itg_in_mds_channel_mapping as(
	select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_IN_MDS_CHANNEL_MAPPING
),
rd as(
		SELECT derived_table1.rn
			,derived_table1.retailer_code
			,derived_table1.customer_code
			,derived_table1.customer_name
			,derived_table1.retailer_name
			,derived_table1.region_code
			,derived_table1.region_name
			,derived_table1.zone_code
			,derived_table1.zone_name
			,derived_table1.zone_classification
			,derived_table1.territory_code
			,derived_table1.territory_name
			,derived_table1.territory_classification
			,derived_table1.channel_code
			,derived_table1.channel_name
			,derived_table1.retailer_category_name
			,derived_table1.class_desc
		FROM (
			SELECT row_number() OVER (
					PARTITION BY ret.customer_code
					,ret.retailer_code ORDER BY ret.end_date DESC
					) AS rn
				,ret.retailer_code
				,ret.customer_code
				,ret.customer_name
				,ret.retailer_name
				,ret.region_code
				,ret.region_name
				,ret.zone_code
				,ret.zone_name
				,ret.zone_classification
				,ret.territory_code
				,ret.territory_name
				,ret.territory_classification
				,ret.channel_code
				,ret.channel_name
				,ret.retailer_category_name
				,ret.class_desc
			FROM edw_retailer_dim ret
			) derived_table1
		WHERE (derived_table1.rn = 1)
),
transformed as(
SELECT rd.customer_code
	,rd.retailer_code
	,rd.customer_name
	,rd.retailer_name
	,rd.channel_name
	,rd.retailer_category_name
	,rd.class_desc
	,COALESCE(cmap.report_channel, 'Missing Channel Mapping'::CHARACTER VARYING) AS report_channel
	,COALESCE(CASE 
			WHEN ((cmap.retailer_channel_level_1)::TEXT = (''::CHARACTER VARYING)::TEXT)
				THEN NULL::CHARACTER VARYING
			ELSE cmap.retailer_channel_level_1
			END, 'Unknown'::CHARACTER VARYING) AS retailer_channel_level1
	,COALESCE(CASE 
			WHEN ((cmap.retailer_channel_level_2)::TEXT = (''::CHARACTER VARYING)::TEXT)
				THEN NULL::CHARACTER VARYING
			ELSE cmap.retailer_channel_level_2
			END, 'Unknown'::CHARACTER VARYING) AS retailer_channel_level2
	,COALESCE(CASE 
			WHEN ((cmap.retailer_channel_level_3)::TEXT = (''::CHARACTER VARYING)::TEXT)
				THEN NULL::CHARACTER VARYING
			ELSE cmap.retailer_channel_level_3
			END, 'Unknown'::CHARACTER VARYING) AS retailer_channel_level3
FROM rd LEFT JOIN itg_in_mds_channel_mapping cmap ON (
			(
				(
					(
						(
							(cmap.channel_name)::TEXT = (
								CASE 
									WHEN (
											CASE 
												WHEN ((rd.channel_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
													THEN NULL::CHARACTER VARYING
												ELSE rd.channel_name
												END IS NULL
											)
										THEN 'Unknown'::CHARACTER VARYING
									ELSE rd.channel_name
									END
								)::TEXT
							)
						AND (
							(cmap.retailer_category_name)::TEXT = (
								CASE 
									WHEN (
											CASE 
												WHEN ((rd.retailer_category_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
													THEN NULL::CHARACTER VARYING
												ELSE rd.retailer_category_name
												END IS NULL
											)
										THEN 'Unknown'::CHARACTER VARYING
									ELSE rd.retailer_category_name
									END
								)::TEXT
							)
						)
					AND (
						(cmap.retailer_class)::TEXT = (
							CASE 
								WHEN (
										CASE 
											WHEN ((rd.class_desc)::TEXT = (''::CHARACTER VARYING)::TEXT)
												THEN NULL::CHARACTER VARYING
											ELSE rd.class_desc
											END IS NULL
										)
									THEN 'Unknown'::CHARACTER VARYING
								ELSE rd.class_desc
								END
							)::TEXT
						)
					)
				AND (
					(cmap.territory_classification)::TEXT = (
						CASE 
							WHEN (
									CASE 
										WHEN ((rd.territory_classification)::TEXT = (''::CHARACTER VARYING)::TEXT)
											THEN NULL::CHARACTER VARYING
										ELSE rd.territory_classification
										END IS NULL
									)
								THEN 'Unknown'::CHARACTER VARYING
							ELSE rd.territory_classification
							END
						)::TEXT
					)
				)
			)

)
select * from transformed