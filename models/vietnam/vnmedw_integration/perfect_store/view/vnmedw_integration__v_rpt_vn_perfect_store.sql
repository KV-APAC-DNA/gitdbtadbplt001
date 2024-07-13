with itg_vn_interface_ise_header as (
select * from {{ ref('vnmitg_integration__itg_vn_interface_ise_header') }}
),
itg_vn_interface_branch as (
select * from {{ ref('vnmitg_integration__itg_vn_interface_branch') }}
),
itg_vn_interface_question as (
select * from {{ ref('vnmitg_integration__itg_vn_interface_question') }}
),
itg_vn_interface_choices as (
select * from {{ ref('vnmitg_integration__itg_vn_interface_choices') }}
),
itg_vn_interface_answers as (
select * from {{ ref('vnmitg_integration__itg_vn_interface_answers') }}
),
itg_vn_interface_notes as (
select * from {{ ref('vnmitg_integration__itg_vn_interface_notes') }}
),
edw_product_key_attributes as (
select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
itg_vn_product_mapping as (
select * from {{ ref('vnmitg_integration__itg_vn_product_mapping') }}
),
itg_mds_vn_ps_targets as (
select * from {{ ref('vnmitg_integration__itg_mds_vn_ps_targets') }}
),
itg_mds_vn_ps_weights as (
select * from {{ ref('vnmitg_integration__itg_mds_vn_ps_weights') }}
),
itg_mds_vn_ps_store_tagging as (
select * from {{ ref('vnmitg_integration__itg_mds_vn_ps_store_tagging') }}
),
union_1 as 		(
			SELECT 'Spiral' AS dataset
				,'Vietnam' AS country
				,survey_master.STATE
				,survey_master.channel
				,survey_master.retail_environment AS storetype
				,survey_master.parent_customer AS storereference
				,survey_master.parent_customer AS salesgroup
				,ans.shop_code AS customerid
				,((((ans.shop_code)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (survey_master.branch_name)::TEXT))::CHARACTER VARYING AS customername
				,ans.slsper_id AS salespersonid
				,ans.slsper_id AS firstname
				,NULL AS lastname
				,to_date((
						(
							(
								(
									"left" (
										ans.visit_date
										,4
										) || ('-'::CHARACTER VARYING)::TEXT
									) || "right" (
									"left" (
										ans.visit_date
										,6
										)
									,2
									)
								) || ('-'::CHARACTER VARYING)::TEXT
							) || "right" (
							ans.visit_date
							,2
							)
						), ('YYYY-MM-DD'::CHARACTER VARYING)::TEXT) AS scheduleddate
				,'COMPLETED' AS vst_status
				,(
					"left" (
						ans.visit_date
						,4
						)
					)::CHARACTER VARYING AS fisc_yr
				,(
					(
						(
							"left" (
								ans.visit_date
								,4
								) || ('0'::CHARACTER VARYING)::TEXT
							) || "right" (
							"left" (
								ans.visit_date
								,6
								)
							,2
							)
						)
					)::CHARACTER VARYING AS fisc_per
				,product_map.gcph_franchise AS prod_hier_l1
				,product_map.gcph_needstate AS prod_hier_l2
				,CASE 
					WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
						THEN product_map.gcph_category
					WHEN (
							(upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
							OR (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
							)
						THEN question.franchise_name
					ELSE choice.brand_name
					END AS prod_hier_l3
				,CASE 
					WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
						THEN product_map.gcph_subcategory
					WHEN (
							(upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
							OR (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
							)
						THEN question.franchise_name
					ELSE choice.brand_name
					END AS prod_hier_l4
				,product_map.gcph_brand AS prod_hier_l5
				,product_map.gcph_brand AS prod_hier_l6
				,((((product_map.gcph_variant)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (product_map.pka_sizedesc)::TEXT))::CHARACTER VARYING AS prod_hier_l7
				,product_map.pka_productdesc AS prod_hier_l8
				,(((ltrim((choice.putup_id)::TEXT, ('0'::CHARACTER VARYING)::TEXT) || (' - '::CHARACTER VARYING)::TEXT) || (product_map.source_system_sku_name)::TEXT))::CHARACTER VARYING AS prod_hier_l9
				,CASE 
					WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
						THEN product_map.gcph_category
					WHEN (
							(upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
							OR (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
							)
						THEN question.franchise_name
					ELSE choice.brand_name
					END AS category
				,CASE 
					WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
						THEN product_map.gcph_subcategory
					WHEN (
							(upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
							OR (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
							)
						THEN question.franchise_name
					ELSE choice.brand_name
					END AS segment
				,(
					CASE 
						WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
							THEN ('MSL COMPLIANCE'::CHARACTER VARYING)::TEXT
						WHEN (upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
							THEN ('PLANOGRAM COMPLIANCE'::CHARACTER VARYING)::TEXT
						WHEN (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
							THEN ('DISPLAY COMPLIANCE'::CHARACTER VARYING)::TEXT
						ELSE upper((question.ques_class_desc)::TEXT)
						END
					)::CHARACTER VARYING AS kpi
				,((((question.ques_desc)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (choice.description)::TEXT))::CHARACTER VARYING AS questiontext
				,NULL AS value
				,CASE 
					WHEN (
							(upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
							AND (ans.score = ((1)::NUMERIC)::NUMERIC(18, 0))
							)
						THEN 'TRUE'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END AS mustcarryitem
				,CASE 
					WHEN (
							(
								(upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
								AND (ans.score = ((1)::NUMERIC)::NUMERIC(18, 0))
								)
							AND (ans.answer_value = ((1)::NUMERIC)::NUMERIC(18, 0))
							)
						THEN 'TRUE'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END AS presence
				,NULL AS outofstock
				,weights.weight AS kpi_chnl_wt
				,targets.target AS mkt_share
				,notes.answer_value AS ques_desc
				,CASE 
					WHEN (
							(
								(upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
								OR (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
								)
							AND (ans.answer_value = ((2)::NUMERIC)::NUMERIC(18, 0))
							)
						THEN 'YES'::CHARACTER VARYING
					WHEN (
							(
								(upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
								OR (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
								)
							AND (ans.answer_value = ((1)::NUMERIC)::NUMERIC(18, 0))
							)
						THEN 'NO'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END AS "y/n_flag"
				,store_tag.STATUS AS priority_store_flag
			FROM (
				(
					(
						(
							(
								(
									(
										(
											(
												SELECT "header".ise_id
													,store.trade_type AS channel
													,store.channel_desc AS retail_environment
													,store.STATE
													,store.parent_cust_name AS parent_customer
													,store.branch_code
													,store.branch_name
													,store.store_prioritization
												FROM (
													itg_vn_interface_ise_header "header" JOIN itg_vn_interface_branch store ON (
															(
																(upper(("header".ise_desc)::TEXT) = upper((store.parent_cust_name)::TEXT))
																AND (upper(("header".channel_code)::TEXT) = upper((store.channel_code)::TEXT))
																)
															)
													)
												GROUP BY "header".ise_id
													,store.trade_type
													,store.channel_desc
													,store.STATE
													,store.parent_cust_name
													,store.branch_code
													,store.branch_name
													,store.store_prioritization
												) survey_master JOIN (
												SELECT itg_vn_interface_question.ise_id
													,itg_vn_interface_question.channel
													,itg_vn_interface_question.ques_no
													,itg_vn_interface_question.ques_code
													,itg_vn_interface_question.ques_desc
													,itg_vn_interface_question.standard_ques
													,itg_vn_interface_question.ques_class_code
													,itg_vn_interface_question.ques_class_desc
													,itg_vn_interface_question.franchise_name
												FROM itg_vn_interface_question
												WHERE (
														(
															(
																(upper((itg_vn_interface_question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
																OR (upper((itg_vn_interface_question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
																)
															OR (upper((itg_vn_interface_question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
															)
														AND (upper((COALESCE(itg_vn_interface_question.franchise_name, 'x'::CHARACTER VARYING))::TEXT) <> ('SECONDARY DISPLAY'::CHARACTER VARYING)::TEXT)
														)
												GROUP BY itg_vn_interface_question.ise_id
													,itg_vn_interface_question.channel
													,itg_vn_interface_question.ques_no
													,itg_vn_interface_question.ques_code
													,itg_vn_interface_question.ques_desc
													,itg_vn_interface_question.standard_ques
													,itg_vn_interface_question.ques_class_code
													,itg_vn_interface_question.ques_class_desc
													,itg_vn_interface_question.franchise_name
												) question ON (((survey_master.ise_id)::TEXT = (question.ise_id)::TEXT))
											) JOIN (
											SELECT itg_vn_interface_choices.ise_id
												,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING AS ques_no
												,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING AS answer_seq
												,itg_vn_interface_choices.putup_id
												,itg_vn_interface_choices.description
												,itg_vn_interface_choices.sfa
												,itg_vn_interface_choices.brand_name
											FROM itg_vn_interface_choices
											GROUP BY itg_vn_interface_choices.ise_id
												,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING
												,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING
												,itg_vn_interface_choices.putup_id
												,itg_vn_interface_choices.description
												,itg_vn_interface_choices.sfa
												,itg_vn_interface_choices.brand_name
											) choice ON (
												(
													((question.ise_id)::TEXT = (choice.ise_id)::TEXT)
													AND (question.ques_no = ((choice.ques_no)::NUMERIC)::NUMERIC(18, 0))
													)
												)
										) JOIN (
										SELECT "left" (
												(itg_vn_interface_answers.createddate)::TEXT
												,8
												) AS visit_date
											,itg_vn_interface_answers.cust_code
											,itg_vn_interface_answers.slsper_id
											,itg_vn_interface_answers.shop_code
											,itg_vn_interface_answers.ise_id
											,itg_vn_interface_answers.ques_no
											,itg_vn_interface_answers.answer_seq
											,itg_vn_interface_answers.answer_value
											,itg_vn_interface_answers.score
											,itg_vn_interface_answers.oos
										FROM itg_vn_interface_answers
										WHERE (itg_vn_interface_answers.score = ((1)::NUMERIC)::NUMERIC(18, 0))
										GROUP BY "left" (
												(itg_vn_interface_answers.createddate)::TEXT
												,8
												)
											,itg_vn_interface_answers.cust_code
											,itg_vn_interface_answers.slsper_id
											,itg_vn_interface_answers.shop_code
											,itg_vn_interface_answers.ise_id
											,itg_vn_interface_answers.ques_no
											,itg_vn_interface_answers.answer_seq
											,itg_vn_interface_answers.answer_value
											,itg_vn_interface_answers.score
											,itg_vn_interface_answers.oos
										) ans ON (
											(
												(
													((choice.ise_id)::TEXT = (ans.ise_id)::TEXT)
													AND ((choice.answer_seq)::TEXT = ((ans.answer_seq)::CHARACTER VARYING)::TEXT)
													)
												AND ((survey_master.branch_code)::TEXT = (ans.shop_code)::TEXT)
												)
											)
									) LEFT JOIN (
									SELECT "left" (
											(itg_vn_interface_notes.createddate)::TEXT
											,8
											) AS visit_date
										,itg_vn_interface_notes.cust_code
										,itg_vn_interface_notes.slsper_id
										,itg_vn_interface_notes.shop_code
										,itg_vn_interface_notes.ise_id
										,itg_vn_interface_notes.ques_no
										,itg_vn_interface_notes.answer_seq
										,itg_vn_interface_notes.answer_value
									FROM itg_vn_interface_notes
									GROUP BY "left" (
											(itg_vn_interface_notes.createddate)::TEXT
											,8
											)
										,itg_vn_interface_notes.cust_code
										,itg_vn_interface_notes.slsper_id
										,itg_vn_interface_notes.shop_code
										,itg_vn_interface_notes.ise_id
										,itg_vn_interface_notes.ques_no
										,itg_vn_interface_notes.answer_seq
										,itg_vn_interface_notes.answer_value
									) notes ON (
										(
											(
												(
													(
														((choice.ise_id)::TEXT = (notes.ise_id)::TEXT)
														AND (ans.answer_seq = (((notes.answer_seq)::CHARACTER VARYING)::NUMERIC)::NUMERIC(18, 0))
														)
													AND (ans.visit_date = notes.visit_date)
													)
												AND (((ans.ques_no)::CHARACTER VARYING)::TEXT = ((notes.ques_no)::CHARACTER VARYING)::TEXT)
												)
											AND ((ans.shop_code)::TEXT = (notes.shop_code)::TEXT)
											)
										)
								) LEFT JOIN (
								SELECT c.putupid
									,a.ean_upc
									,a.gcph_franchise
									,a.gcph_needstate
									,a.gcph_category
									,a.gcph_subcategory
									,a.gcph_segment
									,a.gcph_subsegment
									,a.gcph_brand
									,a.gcph_variant
									,a.pka_sizedesc
									,a.pka_package
									,a.pka_packagedesc
									,a.pka_skuiddesc
									,a.pka_rootcode
									,a.pka_productdesc
									,c.productname AS source_system_sku_name
								FROM (
									(
										(
											SELECT edw_product_key_attributes.ctry_nm
												,edw_product_key_attributes.gcph_franchise
												,edw_product_key_attributes.gcph_needstate
												,edw_product_key_attributes.gcph_category
												,edw_product_key_attributes.gcph_subcategory
												,edw_product_key_attributes.gcph_segment
												,edw_product_key_attributes.gcph_subsegment
												,edw_product_key_attributes.gcph_brand
												,edw_product_key_attributes.gcph_variant
												,edw_product_key_attributes.pka_sizedesc
												,edw_product_key_attributes.pka_package
												,edw_product_key_attributes.pka_packagedesc
												,edw_product_key_attributes.pka_skuiddesc
												,edw_product_key_attributes.pka_rootcode
												,edw_product_key_attributes.pka_productdesc
												,ltrim((edw_product_key_attributes.ean_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT) AS ean_upc
												,edw_product_key_attributes.lst_nts AS nts_date
											FROM edw_product_key_attributes
											WHERE (
													(
														((edw_product_key_attributes.matl_type_cd)::TEXT = ('FERT'::CHARACTER VARYING)::TEXT)
														AND (edw_product_key_attributes.lst_nts IS NOT NULL)
														)
													AND ((edw_product_key_attributes.ctry_nm)::TEXT = ('Vietnam'::CHARACTER VARYING)::TEXT)
													)
											GROUP BY edw_product_key_attributes.ctry_nm
												,edw_product_key_attributes.gcph_franchise
												,edw_product_key_attributes.gcph_needstate
												,edw_product_key_attributes.gcph_category
												,edw_product_key_attributes.gcph_subcategory
												,edw_product_key_attributes.gcph_segment
												,edw_product_key_attributes.gcph_subsegment
												,edw_product_key_attributes.gcph_brand
												,edw_product_key_attributes.gcph_variant
												,edw_product_key_attributes.pka_sizedesc
												,edw_product_key_attributes.pka_package
												,edw_product_key_attributes.pka_packagedesc
												,edw_product_key_attributes.pka_skuiddesc
												,edw_product_key_attributes.pka_rootcode
												,edw_product_key_attributes.pka_productdesc
												,ltrim((edw_product_key_attributes.ean_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT)
												,edw_product_key_attributes.lst_nts
											) a JOIN (
											SELECT edw_product_key_attributes.ctry_nm
												,ltrim((edw_product_key_attributes.ean_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT) AS ean_upc
												,edw_product_key_attributes.lst_nts AS latest_nts_date
												,row_number() OVER (
													PARTITION BY edw_product_key_attributes.ctry_nm
													,edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.lst_nts DESC
													) AS row_number
											FROM edw_product_key_attributes
											WHERE (
													((edw_product_key_attributes.matl_type_cd)::TEXT = ('FERT'::CHARACTER VARYING)::TEXT)
													AND (edw_product_key_attributes.lst_nts IS NOT NULL)
													)
											GROUP BY edw_product_key_attributes.ctry_nm
												,edw_product_key_attributes.ean_upc
												,edw_product_key_attributes.lst_nts
											) b ON (
												(
													(
														(
															((a.ctry_nm)::TEXT = (b.ctry_nm)::TEXT)
															AND (a.ean_upc = b.ean_upc)
															)
														AND (b.latest_nts_date = a.nts_date)
														)
													AND (b.row_number = 1)
													)
												)
										) JOIN itg_vn_product_mapping c ON ((a.ean_upc = (c.barcode)::TEXT))
									)
								GROUP BY c.putupid
									,a.ean_upc
									,a.gcph_franchise
									,a.gcph_needstate
									,a.gcph_category
									,a.gcph_subcategory
									,a.gcph_segment
									,a.gcph_subsegment
									,a.gcph_brand
									,a.gcph_variant
									,a.pka_sizedesc
									,a.pka_package
									,a.pka_packagedesc
									,a.pka_skuiddesc
									,a.pka_rootcode
									,a.pka_productdesc
									,c.productname
								) product_map ON ((ltrim((choice.putup_id)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = product_map.ean_upc))
							) LEFT JOIN itg_mds_vn_ps_targets targets ON (
								(
									(
										(
											(
												CASE 
													WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
														THEN 'MSL COMPLIANCE'::CHARACTER VARYING
													WHEN (upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
														THEN 'PLANOGRAM COMPLIANCE'::CHARACTER VARYING
													WHEN (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
														THEN 'DISPLAY COMPLIANCE'::CHARACTER VARYING
													ELSE 'NA'::CHARACTER VARYING
													END
												)::TEXT = upper((targets.kpi)::TEXT)
											)
										AND ((targets.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
										)
									AND (upper((targets.attribute_1)::TEXT) = upper((question.franchise_name)::TEXT))
									)
								)
						) LEFT JOIN itg_mds_vn_ps_weights weights ON (
							(
								(
									(
										CASE 
											WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
												THEN 'MSL COMPLIANCE'::CHARACTER VARYING
											WHEN (upper((question.ques_class_code)::TEXT) = ('POG'::CHARACTER VARYING)::TEXT)
												THEN 'PLANOGRAM COMPLIANCE'::CHARACTER VARYING
											WHEN (upper((question.ques_class_code)::TEXT) = ('OOL'::CHARACTER VARYING)::TEXT)
												THEN 'DISPLAY COMPLIANCE'::CHARACTER VARYING
											ELSE 'NA'::CHARACTER VARYING
											END
										)::TEXT = upper((weights.kpi)::TEXT)
									)
								AND ((weights.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
								)
							)
					) LEFT JOIN itg_mds_vn_ps_store_tagging store_tag ON (
						(
							((store_tag.parent_customer)::TEXT = (survey_master.parent_customer)::TEXT)
							AND CASE 
								WHEN (
										(
											COALESCE(CASE 
													WHEN ((store_tag.store_code)::TEXT = (''::CHARACTER VARYING)::TEXT)
														THEN NULL::CHARACTER VARYING
													ELSE store_tag.store_code
													END, 'xx'::CHARACTER VARYING)
											)::TEXT <> ('xx'::CHARACTER VARYING)::TEXT
										)
									THEN ((store_tag.store_code)::TEXT = (survey_master.branch_code)::TEXT)
								ELSE (1 = 1)
								END
							)
						)
				)
			
			UNION ALL
			
			SELECT 'Spiral' AS dataset
				,'Vietnam' AS country
				,survey_master.STATE
				,survey_master.channel
				,survey_master.retail_environment AS storetype
				,survey_master.parent_customer AS storereference
				,survey_master.parent_customer AS salesgroup
				,ans.shop_code AS customerid
				,((((ans.shop_code)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (survey_master.branch_name)::TEXT))::CHARACTER VARYING AS customername
				,ans.slsper_id AS salespersonid
				,ans.slsper_id AS firstname
				,NULL AS lastname
				,to_date((
						(
							(
								(
									"left" (
										ans.visit_date
										,4
										) || ('-'::CHARACTER VARYING)::TEXT
									) || "right" (
									"left" (
										ans.visit_date
										,6
										)
									,2
									)
								) || ('-'::CHARACTER VARYING)::TEXT
							) || "right" (
							ans.visit_date
							,2
							)
						), ('YYYY-MM-DD'::CHARACTER VARYING)::TEXT) AS scheduleddate
				,'COMPLETED' AS vst_status
				,(
					"left" (
						ans.visit_date
						,4
						)
					)::CHARACTER VARYING AS fisc_yr
				,(
					(
						(
							"left" (
								ans.visit_date
								,4
								) || ('0'::CHARACTER VARYING)::TEXT
							) || "right" (
							"left" (
								ans.visit_date
								,6
								)
							,2
							)
						)
					)::CHARACTER VARYING AS fisc_per
				,product_map.gcph_franchise AS prod_hier_l1
				,product_map.gcph_needstate AS prod_hier_l2
				,product_map.gcph_category AS prod_hier_l3
				,product_map.gcph_subcategory AS prod_hier_l4
				,product_map.gcph_brand AS prod_hier_l5
				,product_map.gcph_brand AS prod_hier_l6
				,((((product_map.gcph_variant)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (product_map.pka_sizedesc)::TEXT))::CHARACTER VARYING AS prod_hier_l7
				,product_map.pka_productdesc AS prod_hier_l8
				,(((ltrim((choice.putup_id)::TEXT, ('0'::CHARACTER VARYING)::TEXT) || (' - '::CHARACTER VARYING)::TEXT) || (product_map.source_system_sku_name)::TEXT))::CHARACTER VARYING AS prod_hier_l9
				,product_map.gcph_category AS category
				,product_map.gcph_subcategory AS segment
				,'OOS COMPLIANCE' AS kpi
				,((((question.ques_desc)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (choice.description)::TEXT))::CHARACTER VARYING AS questiontext
				,NULL AS value
				,'TRUE' AS mustcarryitem
				,'TRUE' AS presence
				,CASE 
					WHEN (ans.oos = ((1)::NUMERIC)::NUMERIC(18, 0))
						THEN 'TRUE'::CHARACTER VARYING
					ELSE NULL::CHARACTER VARYING
					END AS outofstock
				,weights.weight AS kpi_chnl_wt
				,targets.target AS mkt_share
				,notes.answer_value AS ques_desc
				,NULL AS "y/n_flag"
				,store_tag.STATUS AS priority_store_flag
			FROM (
				(
					(
						(
							(
								(
									(
										(
											(
												SELECT "header".ise_id
													,store.trade_type AS channel
													,store.channel_desc AS retail_environment
													,store.STATE
													,store.parent_cust_name AS parent_customer
													,store.branch_code
													,store.branch_name
													,store.store_prioritization
												FROM (
													itg_vn_interface_ise_header "header" JOIN itg_vn_interface_branch store ON (
															(
																(upper(("header".ise_desc)::TEXT) = upper((store.parent_cust_name)::TEXT))
																AND (upper(("header".channel_code)::TEXT) = upper((store.channel_code)::TEXT))
																)
															)
													)
												GROUP BY "header".ise_id
													,store.trade_type
													,store.channel_desc
													,store.STATE
													,store.parent_cust_name
													,store.branch_code
													,store.branch_name
													,store.store_prioritization
												) survey_master JOIN (
												SELECT itg_vn_interface_question.ise_id
													,itg_vn_interface_question.channel
													,itg_vn_interface_question.ques_no
													,itg_vn_interface_question.ques_code
													,itg_vn_interface_question.ques_desc
													,itg_vn_interface_question.standard_ques
													,itg_vn_interface_question.ques_class_code
													,itg_vn_interface_question.ques_class_desc
													,itg_vn_interface_question.franchise_name
												FROM itg_vn_interface_question
												WHERE ((itg_vn_interface_question.ques_class_code)::TEXT = ('MSL'::CHARACTER VARYING)::TEXT)
												GROUP BY itg_vn_interface_question.ise_id
													,itg_vn_interface_question.channel
													,itg_vn_interface_question.ques_no
													,itg_vn_interface_question.ques_code
													,itg_vn_interface_question.ques_desc
													,itg_vn_interface_question.standard_ques
													,itg_vn_interface_question.ques_class_code
													,itg_vn_interface_question.ques_class_desc
													,itg_vn_interface_question.franchise_name
												) question ON (((survey_master.ise_id)::TEXT = (question.ise_id)::TEXT))
											) JOIN (
											SELECT itg_vn_interface_choices.ise_id
												,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING AS ques_no
												,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING AS answer_seq
												,itg_vn_interface_choices.putup_id
												,itg_vn_interface_choices.description
												,itg_vn_interface_choices.sfa
												,itg_vn_interface_choices.brand_name
											FROM itg_vn_interface_choices
											GROUP BY itg_vn_interface_choices.ise_id
												,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING
												,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING
												,itg_vn_interface_choices.putup_id
												,itg_vn_interface_choices.description
												,itg_vn_interface_choices.sfa
												,itg_vn_interface_choices.brand_name
											) choice ON (
												(
													((question.ise_id)::TEXT = (choice.ise_id)::TEXT)
													AND (question.ques_no = ((choice.ques_no)::NUMERIC)::NUMERIC(18, 0))
													)
												)
										) JOIN (
										SELECT "left" (
												(itg_vn_interface_answers.createddate)::TEXT
												,8
												) AS visit_date
											,itg_vn_interface_answers.cust_code
											,itg_vn_interface_answers.slsper_id
											,itg_vn_interface_answers.shop_code
											,itg_vn_interface_answers.ise_id
											,itg_vn_interface_answers.ques_no
											,itg_vn_interface_answers.answer_seq
											,itg_vn_interface_answers.answer_value
											,itg_vn_interface_answers.score
											,itg_vn_interface_answers.oos
										FROM itg_vn_interface_answers
										WHERE (
												(itg_vn_interface_answers.answer_value = ((1)::NUMERIC)::NUMERIC(18, 0))
												AND (itg_vn_interface_answers.score = ((1)::NUMERIC)::NUMERIC(18, 0))
												)
										GROUP BY "left" (
												(itg_vn_interface_answers.createddate)::TEXT
												,8
												)
											,itg_vn_interface_answers.cust_code
											,itg_vn_interface_answers.slsper_id
											,itg_vn_interface_answers.shop_code
											,itg_vn_interface_answers.ise_id
											,itg_vn_interface_answers.ques_no
											,itg_vn_interface_answers.answer_seq
											,itg_vn_interface_answers.answer_value
											,itg_vn_interface_answers.score
											,itg_vn_interface_answers.oos
										) ans ON (
											(
												(
													((choice.ise_id)::TEXT = (ans.ise_id)::TEXT)
													AND ((choice.answer_seq)::TEXT = ((ans.answer_seq)::CHARACTER VARYING)::TEXT)
													)
												AND ((survey_master.branch_code)::TEXT = (ans.shop_code)::TEXT)
												)
											)
									) LEFT JOIN (
									SELECT "left" (
											(itg_vn_interface_notes.createddate)::TEXT
											,8
											) AS visit_date
										,itg_vn_interface_notes.cust_code
										,itg_vn_interface_notes.slsper_id
										,itg_vn_interface_notes.shop_code
										,itg_vn_interface_notes.ise_id
										,itg_vn_interface_notes.ques_no
										,itg_vn_interface_notes.answer_seq
										,itg_vn_interface_notes.answer_value
									FROM itg_vn_interface_notes
									GROUP BY "left" (
											(itg_vn_interface_notes.createddate)::TEXT
											,8
											)
										,itg_vn_interface_notes.cust_code
										,itg_vn_interface_notes.slsper_id
										,itg_vn_interface_notes.shop_code
										,itg_vn_interface_notes.ise_id
										,itg_vn_interface_notes.ques_no
										,itg_vn_interface_notes.answer_seq
										,itg_vn_interface_notes.answer_value
									) notes ON (
										(
											(
												(
													(
														((choice.ise_id)::TEXT = (notes.ise_id)::TEXT)
														AND (ans.answer_seq = (((notes.answer_seq)::CHARACTER VARYING)::NUMERIC)::NUMERIC(18, 0))
														)
													AND (ans.visit_date = notes.visit_date)
													)
												AND (((ans.ques_no)::CHARACTER VARYING)::TEXT = ((notes.ques_no)::CHARACTER VARYING)::TEXT)
												)
											AND ((ans.shop_code)::TEXT = (notes.shop_code)::TEXT)
											)
										)
								) LEFT JOIN (
								SELECT c.putupid
									,a.ean_upc
									,a.gcph_franchise
									,a.gcph_needstate
									,a.gcph_category
									,a.gcph_subcategory
									,a.gcph_segment
									,a.gcph_subsegment
									,a.gcph_brand
									,a.gcph_variant
									,a.pka_sizedesc
									,a.pka_package
									,a.pka_packagedesc
									,a.pka_skuiddesc
									,a.pka_rootcode
									,a.pka_productdesc
									,c.productname AS source_system_sku_name
								FROM (
									(
										(
											SELECT edw_product_key_attributes.ctry_nm
												,edw_product_key_attributes.gcph_franchise
												,edw_product_key_attributes.gcph_needstate
												,edw_product_key_attributes.gcph_category
												,edw_product_key_attributes.gcph_subcategory
												,edw_product_key_attributes.gcph_segment
												,edw_product_key_attributes.gcph_subsegment
												,edw_product_key_attributes.gcph_brand
												,edw_product_key_attributes.gcph_variant
												,edw_product_key_attributes.pka_sizedesc
												,edw_product_key_attributes.pka_package
												,edw_product_key_attributes.pka_packagedesc
												,edw_product_key_attributes.pka_skuiddesc
												,edw_product_key_attributes.pka_rootcode
												,edw_product_key_attributes.pka_productdesc
												,ltrim((edw_product_key_attributes.ean_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT) AS ean_upc
												,edw_product_key_attributes.lst_nts AS nts_date
											FROM edw_product_key_attributes
											WHERE (
													(
														((edw_product_key_attributes.matl_type_cd)::TEXT = ('FERT'::CHARACTER VARYING)::TEXT)
														AND (edw_product_key_attributes.lst_nts IS NOT NULL)
														)
													AND ((edw_product_key_attributes.ctry_nm)::TEXT = ('Vietnam'::CHARACTER VARYING)::TEXT)
													)
											GROUP BY edw_product_key_attributes.ctry_nm
												,edw_product_key_attributes.gcph_franchise
												,edw_product_key_attributes.gcph_needstate
												,edw_product_key_attributes.gcph_category
												,edw_product_key_attributes.gcph_subcategory
												,edw_product_key_attributes.gcph_segment
												,edw_product_key_attributes.gcph_subsegment
												,edw_product_key_attributes.gcph_brand
												,edw_product_key_attributes.gcph_variant
												,edw_product_key_attributes.pka_sizedesc
												,edw_product_key_attributes.pka_package
												,edw_product_key_attributes.pka_packagedesc
												,edw_product_key_attributes.pka_skuiddesc
												,edw_product_key_attributes.pka_rootcode
												,edw_product_key_attributes.pka_productdesc
												,ltrim((edw_product_key_attributes.ean_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT)
												,edw_product_key_attributes.lst_nts
											) a JOIN (
											SELECT edw_product_key_attributes.ctry_nm
												,ltrim((edw_product_key_attributes.ean_upc)::TEXT, ('0'::CHARACTER VARYING)::TEXT) AS ean_upc
												,edw_product_key_attributes.lst_nts AS latest_nts_date
												,row_number() OVER (
													PARTITION BY edw_product_key_attributes.ctry_nm
													,edw_product_key_attributes.ean_upc ORDER BY edw_product_key_attributes.lst_nts DESC
													) AS row_number
											FROM edw_product_key_attributes
											WHERE (
													((edw_product_key_attributes.matl_type_cd)::TEXT = ('FERT'::CHARACTER VARYING)::TEXT)
													AND (edw_product_key_attributes.lst_nts IS NOT NULL)
													)
											GROUP BY edw_product_key_attributes.ctry_nm
												,edw_product_key_attributes.ean_upc
												,edw_product_key_attributes.lst_nts
											) b ON (
												(
													(
														(
															((a.ctry_nm)::TEXT = (b.ctry_nm)::TEXT)
															AND (a.ean_upc = b.ean_upc)
															)
														AND (b.latest_nts_date = a.nts_date)
														)
													AND (b.row_number = 1)
													)
												)
										) LEFT JOIN itg_vn_product_mapping c ON ((a.ean_upc = (c.barcode)::TEXT))
									)
								GROUP BY c.putupid
									,a.ean_upc
									,a.gcph_franchise
									,a.gcph_needstate
									,a.gcph_category
									,a.gcph_subcategory
									,a.gcph_segment
									,a.gcph_subsegment
									,a.gcph_brand
									,a.gcph_variant
									,a.pka_sizedesc
									,a.pka_package
									,a.pka_packagedesc
									,a.pka_skuiddesc
									,a.pka_rootcode
									,a.pka_productdesc
									,c.productname
								) product_map ON ((ltrim((choice.putup_id)::TEXT, ('0'::CHARACTER VARYING)::TEXT) = product_map.ean_upc))
							) LEFT JOIN itg_mds_vn_ps_targets targets ON (
								(
									(
										(
											(
												CASE 
													WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
														THEN 'OOS COMPLIANCE'::CHARACTER VARYING
													ELSE 'NA'::CHARACTER VARYING
													END
												)::TEXT = upper((targets.kpi)::TEXT)
											)
										AND ((targets.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
										)
									AND (upper((targets.attribute_1)::TEXT) = upper((question.franchise_name)::TEXT))
									)
								)
						) LEFT JOIN itg_mds_vn_ps_weights weights ON (
							(
								(
									(
										CASE 
											WHEN (upper((question.ques_class_code)::TEXT) = ('MSL'::CHARACTER VARYING)::TEXT)
												THEN 'OOS COMPLIANCE'::CHARACTER VARYING
											ELSE 'NA'::CHARACTER VARYING
											END
										)::TEXT = upper((weights.kpi)::TEXT)
									)
								AND ((weights.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
								)
							)
					) LEFT JOIN itg_mds_vn_ps_store_tagging store_tag ON (
						(
							((store_tag.parent_customer)::TEXT = (survey_master.parent_customer)::TEXT)
							AND CASE 
								WHEN (
										(
											COALESCE(CASE 
													WHEN ((store_tag.store_code)::TEXT = (''::CHARACTER VARYING)::TEXT)
														THEN NULL::CHARACTER VARYING
													ELSE store_tag.store_code
													END, 'xx'::CHARACTER VARYING)
											)::TEXT <> ('xx'::CHARACTER VARYING)::TEXT
										)
									THEN ((store_tag.store_code)::TEXT = (survey_master.branch_code)::TEXT)
								ELSE (1 = 1)
								END
							)
						)
				)
			),
union_2 as (SELECT 'Spiral' AS dataset
			,'Vietnam' AS country
			,survey_master.STATE
			,survey_master.channel
			,survey_master.retail_environment AS storetype
			,survey_master.parent_customer AS storereference
			,survey_master.parent_customer AS salesgroup
			,ans.shop_code AS customerid
			,((((ans.shop_code)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (survey_master.branch_name)::TEXT))::CHARACTER VARYING AS customername
			,ans.slsper_id AS salespersonid
			,ans.slsper_id AS firstname
			,NULL AS lastname
			,to_date((
					(
						(
							(
								"left" (
									((ans.visit_date)::CHARACTER VARYING)::TEXT
									,4
									) || ('-'::CHARACTER VARYING)::TEXT
								) || "right" (
								"left" (
									((ans.visit_date)::CHARACTER VARYING)::TEXT
									,6
									)
								,2
								)
							) || ('-'::CHARACTER VARYING)::TEXT
						) || "right" (
						((ans.visit_date)::CHARACTER VARYING)::TEXT
						,2
						)
					), ('YYYY-MM-DD'::CHARACTER VARYING)::TEXT) AS scheduleddate
			,'COMPLETED' AS vst_status
			,(
				"left" (
					((ans.visit_date)::CHARACTER VARYING)::TEXT
					,4
					)
				)::CHARACTER VARYING AS fisc_yr
			,(
				(
					(
						(
							(
								"left" (
									((ans.visit_date)::CHARACTER VARYING)::TEXT
									,4
									)
								)::CHARACTER VARYING
							)::TEXT || ('0'::CHARACTER VARYING)::TEXT
						) || (
						(
							"right" (
								"left" (
									((ans.visit_date)::CHARACTER VARYING)::TEXT
									,6
									)
								,2
								)
							)::CHARACTER VARYING
						)::TEXT
					)
				)::CHARACTER VARYING AS fisc_per
			,NULL AS prod_hier_l1
			,NULL AS prod_hier_l2
			,question.franchise_name AS prod_hier_l3
			,choice.brand_name AS prod_hier_l4
			,choice.brand_name AS prod_hier_l5
			,NULL AS prod_hier_l6
			,NULL AS prod_hier_l7
			,NULL AS prod_hier_l8
			,NULL AS prod_hier_l9
			,question.franchise_name AS category
			,COALESCE(CASE 
					WHEN ((choice.brand_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
						THEN NULL::CHARACTER VARYING
					ELSE choice.brand_name
					END, question.franchise_name) AS segment
			,(upper((question.ques_class_desc)::TEXT))::CHARACTER VARYING AS kpi
			,((((question.ques_desc)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (choice.description)::TEXT))::CHARACTER VARYING AS questiontext
			,(ans.answer_value)::CHARACTER VARYING AS value
			,NULL AS mustcarryitem
			,NULL AS presence
			,NULL AS outofstock
			,weights.weight AS kpi_chnl_wt
			,targets.target AS mkt_share
			,'NUMERATOR'::CHARACTER VARYING AS ques_desc
			,NULL AS "y/n_flag"
			,store_tag.STATUS AS priority_store_flag
		FROM (
			(
				(
					(
						(
							(
								(
									(
										SELECT "header".ise_id
											,store.trade_type AS channel
											,store.channel_desc AS retail_environment
											,store.STATE
											,store.parent_cust_name AS parent_customer
											,store.branch_code
											,store.branch_name
											,store.store_prioritization
										FROM (
											itg_vn_interface_ise_header "header" JOIN itg_vn_interface_branch store ON (
													(
														(upper(("header".ise_desc)::TEXT) = upper((store.parent_cust_name)::TEXT))
														AND (upper(("header".channel_code)::TEXT) = upper((store.channel_code)::TEXT))
														)
													)
											)
										GROUP BY "header".ise_id
											,store.trade_type
											,store.channel_desc
											,store.STATE
											,store.parent_cust_name
											,store.branch_code
											,store.branch_name
											,store.store_prioritization
										) survey_master JOIN (
										SELECT itg_vn_interface_question.ise_id
											,itg_vn_interface_question.channel
											,itg_vn_interface_question.ques_no
											,itg_vn_interface_question.ques_code
											,itg_vn_interface_question.ques_desc
											,itg_vn_interface_question.standard_ques
											,itg_vn_interface_question.ques_class_code
											,itg_vn_interface_question.ques_class_desc
											,itg_vn_interface_question.franchise_name
										FROM itg_vn_interface_question
										WHERE ((itg_vn_interface_question.ques_class_code)::TEXT = ('SOS'::CHARACTER VARYING)::TEXT)
										GROUP BY itg_vn_interface_question.ise_id
											,itg_vn_interface_question.channel
											,itg_vn_interface_question.ques_no
											,itg_vn_interface_question.ques_code
											,itg_vn_interface_question.ques_desc
											,itg_vn_interface_question.standard_ques
											,itg_vn_interface_question.ques_class_code
											,itg_vn_interface_question.ques_class_desc
											,itg_vn_interface_question.franchise_name
										) question ON (((survey_master.ise_id)::TEXT = (question.ise_id)::TEXT))
									) JOIN (
									SELECT base.ise_id
										,(base.ques_no)::CHARACTER VARYING AS ques_no
										,(base.answer_seq)::CHARACTER VARYING AS answer_seq
										,base.putup_id
										,base.description
										,base.sfa
										,base.brand_name
									FROM (
										itg_vn_interface_choices base JOIN (
											SELECT question.ise_id
												,question.franchise_name
												,min((choice.answer_seq)::TEXT) AS jj_answer_seq
											FROM (
												(
													SELECT itg_vn_interface_question.ise_id
														,itg_vn_interface_question.channel
														,itg_vn_interface_question.ques_no
														,itg_vn_interface_question.ques_code
														,itg_vn_interface_question.ques_desc
														,itg_vn_interface_question.standard_ques
														,itg_vn_interface_question.ques_class_code
														,itg_vn_interface_question.ques_class_desc
														,itg_vn_interface_question.franchise_name
													FROM itg_vn_interface_question
													WHERE ((itg_vn_interface_question.ques_class_code)::TEXT = ('SOS'::CHARACTER VARYING)::TEXT)
													GROUP BY itg_vn_interface_question.ise_id
														,itg_vn_interface_question.channel
														,itg_vn_interface_question.ques_no
														,itg_vn_interface_question.ques_code
														,itg_vn_interface_question.ques_desc
														,itg_vn_interface_question.standard_ques
														,itg_vn_interface_question.ques_class_code
														,itg_vn_interface_question.ques_class_desc
														,itg_vn_interface_question.franchise_name
													) question JOIN (
													SELECT itg_vn_interface_choices.ise_id
														,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING AS ques_no
														,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING AS answer_seq
														,itg_vn_interface_choices.putup_id
														,itg_vn_interface_choices.description
														,itg_vn_interface_choices.sfa
														,itg_vn_interface_choices.brand_name
													FROM itg_vn_interface_choices
													GROUP BY itg_vn_interface_choices.ise_id
														,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING
														,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING
														,itg_vn_interface_choices.putup_id
														,itg_vn_interface_choices.description
														,itg_vn_interface_choices.sfa
														,itg_vn_interface_choices.brand_name
													) choice ON (
														(
															((question.ise_id)::TEXT = (choice.ise_id)::TEXT)
															AND (question.ques_no = ((choice.ques_no)::NUMERIC)::NUMERIC(18, 0))
															)
														)
												)
											GROUP BY question.ise_id
												,question.franchise_name
											) ref ON (
												(
													((base.ise_id)::TEXT = (ref.ise_id)::TEXT)
													AND (((base.answer_seq)::CHARACTER VARYING)::TEXT = ((ref.jj_answer_seq)::CHARACTER VARYING)::TEXT)
													)
												)
										)
									GROUP BY base.ise_id
										,(base.ques_no)::CHARACTER VARYING
										,(base.answer_seq)::CHARACTER VARYING
										,base.putup_id
										,base.description
										,base.sfa
										,base.brand_name
									) choice ON (
										(
											((question.ise_id)::TEXT = (choice.ise_id)::TEXT)
											AND (question.ques_no = ((choice.ques_no)::NUMERIC)::NUMERIC(18, 0))
											)
										)
								) JOIN (
								SELECT "left" (
										(itg_vn_interface_answers.createddate)::TEXT
										,8
										) AS visit_date
									,itg_vn_interface_answers.cust_code
									,itg_vn_interface_answers.slsper_id
									,itg_vn_interface_answers.shop_code
									,itg_vn_interface_answers.ise_id
									,itg_vn_interface_answers.ques_no
									,itg_vn_interface_answers.answer_seq
									,itg_vn_interface_answers.answer_value
									,itg_vn_interface_answers.score
									,itg_vn_interface_answers.oos
								FROM itg_vn_interface_answers
								WHERE (itg_vn_interface_answers.answer_value > ((0)::NUMERIC)::NUMERIC(18, 0))
								GROUP BY "left" (
										(itg_vn_interface_answers.createddate)::TEXT
										,8
										)
									,itg_vn_interface_answers.cust_code
									,itg_vn_interface_answers.slsper_id
									,itg_vn_interface_answers.shop_code
									,itg_vn_interface_answers.ise_id
									,itg_vn_interface_answers.ques_no
									,itg_vn_interface_answers.answer_seq
									,itg_vn_interface_answers.answer_value
									,itg_vn_interface_answers.score
									,itg_vn_interface_answers.oos
								) ans ON (
									(
										(
											((choice.ise_id)::TEXT = (ans.ise_id)::TEXT)
											AND ((choice.answer_seq)::TEXT = ((ans.answer_seq)::CHARACTER VARYING)::TEXT)
											)
										AND ((survey_master.branch_code)::TEXT = (ans.shop_code)::TEXT)
										)
									)
							) LEFT JOIN (
							SELECT "left" (
									(itg_vn_interface_notes.createddate)::TEXT
									,8
									) AS visit_date
								,itg_vn_interface_notes.cust_code
								,itg_vn_interface_notes.slsper_id
								,itg_vn_interface_notes.shop_code
								,itg_vn_interface_notes.ise_id
								,itg_vn_interface_notes.ques_no
								,itg_vn_interface_notes.answer_seq
								,itg_vn_interface_notes.answer_value
							FROM itg_vn_interface_notes
							GROUP BY "left" (
									(itg_vn_interface_notes.createddate)::TEXT
									,8
									)
								,itg_vn_interface_notes.cust_code
								,itg_vn_interface_notes.slsper_id
								,itg_vn_interface_notes.shop_code
								,itg_vn_interface_notes.ise_id
								,itg_vn_interface_notes.ques_no
								,itg_vn_interface_notes.answer_seq
								,itg_vn_interface_notes.answer_value
							) notes ON (
								(
									(
										(
											(
												((choice.ise_id)::TEXT = (notes.ise_id)::TEXT)
												AND (ans.answer_seq = (((notes.answer_seq)::CHARACTER VARYING)::NUMERIC)::NUMERIC(18, 0))
												)
											AND (ans.visit_date = notes.visit_date)
											)
										AND (((ans.ques_no)::CHARACTER VARYING)::TEXT = ((notes.ques_no)::CHARACTER VARYING)::TEXT)
										)
									AND ((ans.shop_code)::TEXT = (notes.shop_code)::TEXT)
									)
								)
						) LEFT JOIN itg_mds_vn_ps_targets targets ON (
							(
								(
									(
										(
											CASE 
												WHEN (
														(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
														AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF SHELF'::CHARACTER VARYING)::TEXT)
														)
													THEN 'SOS COMPLIANCE'::CHARACTER VARYING
												WHEN (
														(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
														AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF ASSORTMENT'::CHARACTER VARYING)::TEXT)
														)
													THEN 'SOA COMPLIANCE'::CHARACTER VARYING
												ELSE 'NA'::CHARACTER VARYING
												END
											)::TEXT = upper((targets.kpi)::TEXT)
										)
									AND ((targets.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
									)
								AND (upper((targets.attribute_1)::TEXT) = upper((question.franchise_name)::TEXT))
								)
							)
					) LEFT JOIN itg_mds_vn_ps_weights weights ON (
						(
							(
								(
									CASE 
										WHEN (
												(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
												AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF SHELF'::CHARACTER VARYING)::TEXT)
												)
											THEN 'SOS COMPLIANCE'::CHARACTER VARYING
										WHEN (
												(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
												AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF ASSORTMENT'::CHARACTER VARYING)::TEXT)
												)
											THEN 'SOA COMPLIANCE'::CHARACTER VARYING
										ELSE 'NA'::CHARACTER VARYING
										END
									)::TEXT = upper((weights.kpi)::TEXT)
								)
							AND ((weights.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
							)
						)
				) LEFT JOIN itg_mds_vn_ps_store_tagging store_tag ON (
					(
						((store_tag.parent_customer)::TEXT = (survey_master.parent_customer)::TEXT)
						AND CASE 
							WHEN (
									(
										COALESCE(CASE 
												WHEN ((store_tag.store_code)::TEXT = (''::CHARACTER VARYING)::TEXT)
													THEN NULL::CHARACTER VARYING
												ELSE store_tag.store_code
												END, 'xx'::CHARACTER VARYING)
										)::TEXT <> ('xx'::CHARACTER VARYING)::TEXT
									)
								THEN ((store_tag.store_code)::TEXT = (survey_master.branch_code)::TEXT)
							ELSE (1 = 1)
							END
						)
					)
			)
		),
union_3 as (
SELECT 'Spiral'::CHARACTER VARYING AS dataset
	,'Vietnam'::CHARACTER VARYING AS country
	,survey_master.STATE
	,survey_master.channel
	,survey_master.retail_environment AS storetype
	,survey_master.parent_customer AS storereference
	,survey_master.parent_customer AS salesgroup
	,ans.shop_code AS customerid
	,((((ans.shop_code)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (survey_master.branch_name)::TEXT))::CHARACTER VARYING AS customername
	,ans.slsper_id AS salespersonid
	,ans.slsper_id AS firstname
	,NULL::CHARACTER VARYING AS lastname
	,to_date((
			(
				(
					(
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,4
							) || ('-'::CHARACTER VARYING)::TEXT
						) || "right" (
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,6
							)
						,2
						)
					) || ('-'::CHARACTER VARYING)::TEXT
				) || "right" (
				((ans.visit_date)::CHARACTER VARYING)::TEXT
				,2
				)
			), ('YYYY-MM-DD'::CHARACTER VARYING)::TEXT) AS scheduleddate
	,'COMPLETED'::CHARACTER VARYING AS vst_status
	,(
		"left" (
			((ans.visit_date)::CHARACTER VARYING)::TEXT
			,4
			)
		)::CHARACTER VARYING AS fisc_yr
	,(
		(
			(
				(
					(
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,4
							)
						)::CHARACTER VARYING
					)::TEXT || ('0'::CHARACTER VARYING)::TEXT
				) || (
				(
					"right" (
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,6
							)
						,2
						)
					)::CHARACTER VARYING
				)::TEXT
			)
		)::CHARACTER VARYING AS fisc_per
	,NULL::CHARACTER VARYING AS prod_hier_l1
	,NULL::CHARACTER VARYING AS prod_hier_l2
	,question.franchise_name AS prod_hier_l3
	,choice.brand_name AS prod_hier_l4
	,choice.brand_name AS prod_hier_l5
	,NULL::CHARACTER VARYING AS prod_hier_l6
	,NULL::CHARACTER VARYING AS prod_hier_l7
	,NULL::CHARACTER VARYING AS prod_hier_l8
	,NULL::CHARACTER VARYING AS prod_hier_l9
	,question.franchise_name AS category
	,COALESCE(CASE 
			WHEN ((choice.brand_name)::TEXT = (''::CHARACTER VARYING)::TEXT)
				THEN NULL::CHARACTER VARYING
			ELSE choice.brand_name
			END, question.franchise_name) AS segment
	,(upper((question.ques_class_desc)::TEXT))::CHARACTER VARYING AS kpi
	,(((('What is the total value of the '::CHARACTER VARYING)::TEXT || (question.franchise_name)::TEXT) || (' franchise?'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS questiontext
	,(sum((ans.answer_value)::DOUBLE PRECISION))::CHARACTER VARYING AS value
	,NULL::CHARACTER VARYING AS mustcarryitem
	,NULL::CHARACTER VARYING AS presence
	,NULL::CHARACTER VARYING AS outofstock
	,weights.weight AS kpi_chnl_wt
	,targets.target AS mkt_share
	,'DENOMINATOR'::CHARACTER VARYING AS ques_desc
	,NULL AS "y/n_flag"
	,store_tag.STATUS AS priority_store_flag
FROM (
	(
		(
			(
				(
					(
						(
							(
								SELECT "header".ise_id
									,store.trade_type AS channel
									,store.channel_desc AS retail_environment
									,store.STATE
									,store.parent_cust_name AS parent_customer
									,store.branch_code
									,store.branch_name
									,store.store_prioritization
								FROM (
									itg_vn_interface_ise_header "header" JOIN itg_vn_interface_branch store ON (
											(
												(upper(("header".ise_desc)::TEXT) = upper((store.parent_cust_name)::TEXT))
												AND (upper(("header".channel_code)::TEXT) = upper((store.channel_code)::TEXT))
												)
											)
									)
								GROUP BY "header".ise_id
									,store.trade_type
									,store.channel_desc
									,store.STATE
									,store.parent_cust_name
									,store.branch_code
									,store.branch_name
									,store.store_prioritization
								) survey_master JOIN (
								SELECT itg_vn_interface_question.ise_id
									,itg_vn_interface_question.channel
									,itg_vn_interface_question.ques_no
									,itg_vn_interface_question.ques_code
									,itg_vn_interface_question.ques_desc
									,itg_vn_interface_question.standard_ques
									,itg_vn_interface_question.ques_class_code
									,itg_vn_interface_question.ques_class_desc
									,itg_vn_interface_question.franchise_name
								FROM itg_vn_interface_question
								WHERE ((itg_vn_interface_question.ques_class_code)::TEXT = ('SOS'::CHARACTER VARYING)::TEXT)
								GROUP BY itg_vn_interface_question.ise_id
									,itg_vn_interface_question.channel
									,itg_vn_interface_question.ques_no
									,itg_vn_interface_question.ques_code
									,itg_vn_interface_question.ques_desc
									,itg_vn_interface_question.standard_ques
									,itg_vn_interface_question.ques_class_code
									,itg_vn_interface_question.ques_class_desc
									,itg_vn_interface_question.franchise_name
								) question ON (((survey_master.ise_id)::TEXT = (question.ise_id)::TEXT))
							) JOIN (
							SELECT itg_vn_interface_choices.ise_id
								,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING AS ques_no
								,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING AS answer_seq
								,itg_vn_interface_choices.putup_id
								,itg_vn_interface_choices.description
								,itg_vn_interface_choices.sfa
								,itg_vn_interface_choices.brand_name
							FROM itg_vn_interface_choices
							GROUP BY itg_vn_interface_choices.ise_id
								,(itg_vn_interface_choices.ques_no)::CHARACTER VARYING
								,(itg_vn_interface_choices.answer_seq)::CHARACTER VARYING
								,itg_vn_interface_choices.putup_id
								,itg_vn_interface_choices.description
								,itg_vn_interface_choices.sfa
								,itg_vn_interface_choices.brand_name
							) choice ON (
								(
									((question.ise_id)::TEXT = (choice.ise_id)::TEXT)
									AND (question.ques_no = ((choice.ques_no)::NUMERIC)::NUMERIC(18, 0))
									)
								)
						) JOIN (
						SELECT "left" (
								(itg_vn_interface_answers.createddate)::TEXT
								,8
								) AS visit_date
							,itg_vn_interface_answers.cust_code
							,itg_vn_interface_answers.slsper_id
							,itg_vn_interface_answers.shop_code
							,itg_vn_interface_answers.ise_id
							,itg_vn_interface_answers.ques_no
							,itg_vn_interface_answers.answer_seq
							,itg_vn_interface_answers.answer_value
							,itg_vn_interface_answers.score
							,itg_vn_interface_answers.oos
						FROM itg_vn_interface_answers
						WHERE (itg_vn_interface_answers.answer_value > ((0)::NUMERIC)::NUMERIC(18, 0))
						GROUP BY "left" (
								(itg_vn_interface_answers.createddate)::TEXT
								,8
								)
							,itg_vn_interface_answers.cust_code
							,itg_vn_interface_answers.slsper_id
							,itg_vn_interface_answers.shop_code
							,itg_vn_interface_answers.ise_id
							,itg_vn_interface_answers.ques_no
							,itg_vn_interface_answers.answer_seq
							,itg_vn_interface_answers.answer_value
							,itg_vn_interface_answers.score
							,itg_vn_interface_answers.oos
						) ans ON (
							(
								(
									((choice.ise_id)::TEXT = (ans.ise_id)::TEXT)
									AND ((choice.answer_seq)::TEXT = ((ans.answer_seq)::CHARACTER VARYING)::TEXT)
									)
								AND ((survey_master.branch_code)::TEXT = (ans.shop_code)::TEXT)
								)
							)
					) LEFT JOIN (
					SELECT "left" (
							(itg_vn_interface_notes.createddate)::TEXT
							,8
							) AS visit_date
						,itg_vn_interface_notes.cust_code
						,itg_vn_interface_notes.slsper_id
						,itg_vn_interface_notes.shop_code
						,itg_vn_interface_notes.ise_id
						,itg_vn_interface_notes.ques_no
						,itg_vn_interface_notes.answer_seq
						,itg_vn_interface_notes.answer_value
					FROM itg_vn_interface_notes
					GROUP BY "left" (
							(itg_vn_interface_notes.createddate)::TEXT
							,8
							)
						,itg_vn_interface_notes.cust_code
						,itg_vn_interface_notes.slsper_id
						,itg_vn_interface_notes.shop_code
						,itg_vn_interface_notes.ise_id
						,itg_vn_interface_notes.ques_no
						,itg_vn_interface_notes.answer_seq
						,itg_vn_interface_notes.answer_value
					) notes ON (
						(
							(
								(
									(
										((choice.ise_id)::TEXT = (notes.ise_id)::TEXT)
										AND (ans.answer_seq = (((notes.answer_seq)::CHARACTER VARYING)::NUMERIC)::NUMERIC(18, 0))
										)
									AND (ans.visit_date = notes.visit_date)
									)
								AND (((ans.ques_no)::CHARACTER VARYING)::TEXT = ((notes.ques_no)::CHARACTER VARYING)::TEXT)
								)
							AND ((ans.shop_code)::TEXT = (notes.shop_code)::TEXT)
							)
						)
				) LEFT JOIN itg_mds_vn_ps_targets targets ON (
					(
						(
							(
								(
									CASE 
										WHEN (
												(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
												AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF SHELF'::CHARACTER VARYING)::TEXT)
												)
											THEN 'SOS COMPLIANCE'::CHARACTER VARYING
										WHEN (
												(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
												AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF ASSORTMENT'::CHARACTER VARYING)::TEXT)
												)
											THEN 'SOA COMPLIANCE'::CHARACTER VARYING
										ELSE 'NA'::CHARACTER VARYING
										END
									)::TEXT = upper((targets.kpi)::TEXT)
								)
							AND ((targets.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
							)
						AND (upper((targets.attribute_1)::TEXT) = upper((question.franchise_name)::TEXT))
						)
					)
			) LEFT JOIN itg_mds_vn_ps_weights weights ON (
				(
					(
						(
							CASE 
								WHEN (
										(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
										AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF SHELF'::CHARACTER VARYING)::TEXT)
										)
									THEN 'SOS COMPLIANCE'::CHARACTER VARYING
								WHEN (
										(upper((question.ques_class_code)::TEXT) = ('SOS'::CHARACTER VARYING)::TEXT)
										AND (upper((question.ques_class_desc)::TEXT) = ('SHARE OF ASSORTMENT'::CHARACTER VARYING)::TEXT)
										)
									THEN 'SOA COMPLIANCE'::CHARACTER VARYING
								ELSE 'NA'::CHARACTER VARYING
								END
							)::TEXT = upper((weights.kpi)::TEXT)
						)
					AND ((weights.retail_env)::TEXT = (survey_master.retail_environment)::TEXT)
					)
				)
		) LEFT JOIN itg_mds_vn_ps_store_tagging store_tag ON (
			(
				((store_tag.parent_customer)::TEXT = (survey_master.parent_customer)::TEXT)
				AND CASE 
					WHEN (
							(
								COALESCE(CASE 
										WHEN ((store_tag.store_code)::TEXT = (''::CHARACTER VARYING)::TEXT)
											THEN NULL::CHARACTER VARYING
										ELSE store_tag.store_code
										END, 'xx'::CHARACTER VARYING)
								)::TEXT <> ('xx'::CHARACTER VARYING)::TEXT
							)
						THEN ((store_tag.store_code)::TEXT = (survey_master.branch_code)::TEXT)
					ELSE (1 = 1)
					END
				)
			)
	)
GROUP BY 1
	,2
	,survey_master.STATE
	,survey_master.channel
	,survey_master.retail_environment
	,survey_master.parent_customer
	,ans.shop_code
	,((((ans.shop_code)::TEXT || (' - '::CHARACTER VARYING)::TEXT) || (survey_master.branch_name)::TEXT))::CHARACTER VARYING
	,ans.slsper_id
	,12
	,to_date((
			(
				(
					(
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,4
							) || ('-'::CHARACTER VARYING)::TEXT
						) || "right" (
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,6
							)
						,2
						)
					) || ('-'::CHARACTER VARYING)::TEXT
				) || "right" (
				((ans.visit_date)::CHARACTER VARYING)::TEXT
				,2
				)
			), ('YYYY-MM-DD'::CHARACTER VARYING)::TEXT)
	,14
	,(
		"left" (
			((ans.visit_date)::CHARACTER VARYING)::TEXT
			,4
			)
		)::CHARACTER VARYING
	,(
		(
			(
				(
					(
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,4
							)
						)::CHARACTER VARYING
					)::TEXT || ('0'::CHARACTER VARYING)::TEXT
				) || (
				(
					"right" (
						"left" (
							((ans.visit_date)::CHARACTER VARYING)::TEXT
							,6
							)
						,2
						)
					)::CHARACTER VARYING
				)::TEXT
			)
		)::CHARACTER VARYING
	,17
	,18
	,question.franchise_name
	,choice.brand_name
	,22
	,23
	,24
	,25
	,(upper((question.ques_class_desc)::TEXT))::CHARACTER VARYING
	,(((('What is the total value of the '::CHARACTER VARYING)::TEXT || (question.franchise_name)::TEXT) || (' franchise?'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING
	,31
	,32
	,33
	,weights.weight
	,targets.target
	,store_tag.STATUS
),
final as ( 
		select * from union_1
		UNION ALL
		select * from union_2		
        UNION ALL
        select * from union_3
)
select * from final