with 
edw_vw_ph_survey_details as (
select * from {{ ref('phledw_integration__edw_vw_ph_survey_details') }}
),
itg_mds_ph_product_hierarchy as (
select * from {{ ref('phlitg_integration__itg_mds_ph_product_hierarchy') }}
),
itg_ph_tbl_isebranchmaster as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_isebranchmaster') }}
),
itg_mds_ph_ise_weights as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ise_weights') }}
),
edw_vw_os_time_dim as (
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
ph_kpi2data_mapping as (
select * from {{ source('phledw_integration','ph_kpi2data_mapping') }}
),
itg_mds_ph_ise_sos_targets as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ise_sos_targets') }}
),
itg_ph_non_ise_msl_osa as (
select * from {{ ref('phlitg_integration__itg_ph_non_ise_msl_osa') }}
),
itg_mds_ph_lav_product as (
select * from {{ ref('phlitg_integration__itg_mds_ph_lav_product') }}
),
itg_mds_ph_pos_customers as (
select * from {{ ref('phlitg_integration__itg_mds_ph_pos_customers') }}
),
itg_ph_ps_retailer_soldto_map as (
select * from {{ ref('phlitg_integration__itg_ph_ps_retailer_soldto_map') }}
),
itg_mds_ph_ise_parent as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ise_parent') }}
),
itg_mds_ph_ref_parent_customer as (
select * from {{ ref('phlitg_integration__itg_mds_ph_ref_parent_customer') }}
),
itg_ph_tbl_surveyisequestion as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_surveyisequestion') }}
),
itg_ph_tbl_surveyisehdr as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_surveyisehdr') }}
),
itg_ph_tbl_surveychoices as (
select * from {{ ref('phlitg_integration__itg_ph_tbl_surveychoices') }}
),
itg_ph_non_ise_weights as (
select * from {{ ref('phlitg_integration__itg_ph_non_ise_weights') }}
),
vw_ph_clobotics_perfect_store as (
select * from {{ ref('phledw_integration__vw_ph_clobotics_perfect_store') }}
),
union_1 as 				(
						SELECT 'Merchandising_Response' AS dataset
							,srv_det.branchcode AS customerid
							,srv_det.slsperid AS salespersonid
							,srv_det.startdate AS mrch_resp_startdt
							,srv_det.enddate AS mrch_resp_enddt
							,NULL AS survey_enddate
							,NULL AS questiontext
							,NULL AS value
							,'true' AS mustcarryitem
							,CASE 
								WHEN srv_det.answerscore = 1
									THEN 'true'::CHARACTER VARYING
								ELSE 'false'::CHARACTER VARYING
								END AS answerscore
							,CASE 
								WHEN srv_det.answerscore <> 0
									OR srv_det.oos <> 0
									THEN 'true'::CHARACTER VARYING
								ELSE 'false'::CHARACTER VARYING
								END AS presence
							,CASE 
								WHEN srv_det.oos = 1
									THEN 'true'::CHARACTER VARYING
								ELSE ''::CHARACTER VARYING
								END AS outofstock
							,'MSL Compliance' AS kpi
							,srv_det.createddate AS scheduleddate
							,'completed' AS vst_status
							,time_dim."year" AS fisc_yr
							,time_dim.mnth_id::CHARACTER VARYING AS fisc_per
							,('(ISE) '::CHARACTER VARYING::TEXT || srv_det.name::TEXT)::CHARACTER VARYING AS firstname
							,'' AS lastname
							,(srv_det.branchcode::TEXT || ' - '::CHARACTER VARYING::TEXT || branch_master.branchname::TEXT)::CHARACTER VARYING AS customername
							,'Philippines' AS country
							,branch_master.area AS storereference
							,branch_master.channel AS storetype
							,branch_master.tradetype AS channel
							,branch_master.parentname AS salesgroup
							,product.franchise_nm AS prod_hier_l3
							,product.brand_nm AS prod_hier_l4
							,product.variant_nm AS prod_hier_l5
							,product.ph_ref_repvarputup_nm AS prod_hier_l6
							,NULL AS category
							,NULL AS segment
							,weight.weight AS kpi_chnl_wt
							,NULL AS mkt_share
							,NULL AS ques_desc
							,NULL AS "y/n_flag"
						FROM (
							SELECT edw_vw_ph_survey_details.branchcode
								,edw_vw_ph_survey_details.slsperid
								,edw_vw_ph_survey_details.custcode
								,edw_vw_ph_survey_details.iseid
								,edw_vw_ph_survey_details.ans_quesno
								,edw_vw_ph_survey_details.answerseq
								,edw_vw_ph_survey_details.answervalue
								,edw_vw_ph_survey_details.answerscore
								,edw_vw_ph_survey_details.oos
								,edw_vw_ph_survey_details.createddate
								,edw_vw_ph_survey_details.storeprioritization
								,edw_vw_ph_survey_details.name
								,edw_vw_ph_survey_details.putupid
								,edw_vw_ph_survey_details.putupdesc
								,edw_vw_ph_survey_details.brandid
								,edw_vw_ph_survey_details.brand
								,edw_vw_ph_survey_details.quesno
								,edw_vw_ph_survey_details.quesclasscode
								,edw_vw_ph_survey_details.quesclassdesc
								,edw_vw_ph_survey_details.totalscore
								,edw_vw_ph_survey_details.franchisecode
								,edw_vw_ph_survey_details.isedesc
								,edw_vw_ph_survey_details.startdate
								,edw_vw_ph_survey_details.enddate
							FROM edw_vw_ph_survey_details
							) srv_det
						LEFT JOIN (
							SELECT DISTINCT itg_mds_ph_product_hierarchy.ph_ref_repvarputup_cd
								,itg_mds_ph_product_hierarchy.franchise_nm
								,itg_mds_ph_product_hierarchy.brand_nm
								,itg_mds_ph_product_hierarchy.variant_nm
								,itg_mds_ph_product_hierarchy.ph_ref_repvarputup_nm
							FROM itg_mds_ph_product_hierarchy
							) product ON srv_det.putupid::TEXT = product.ph_ref_repvarputup_cd::TEXT
						LEFT JOIN (
							SELECT DISTINCT itg_ph_tbl_isebranchmaster.branchcode
								,itg_ph_tbl_isebranchmaster.branchname
								,itg_ph_tbl_isebranchmaster.area
								,itg_ph_tbl_isebranchmaster.channel
								,itg_ph_tbl_isebranchmaster.parentcode
								,itg_ph_tbl_isebranchmaster.parentname
								,itg_ph_tbl_isebranchmaster.tradetype
							FROM itg_ph_tbl_isebranchmaster
							) branch_master ON srv_det.custcode::TEXT = branch_master.parentcode::TEXT
							AND srv_det.branchcode::TEXT = branch_master.branchcode::TEXT
						LEFT JOIN (
							SELECT DISTINCT itg_mds_ph_ise_weights.iseid
								,UPPER(trim(itg_mds_ph_ise_weights.kpi::TEXT)) AS kpi
								,itg_mds_ph_ise_weights.weight / 100::NUMERIC::NUMERIC(18, 0) AS weight
							FROM itg_mds_ph_ise_weights
							) weight ON UPPER(trim(srv_det.quesclassdesc::TEXT)) like (('%'::CHARACTER VARYING::TEXT || weight.kpi) || '%'::CHARACTER VARYING::TEXT)
							AND srv_det.iseid::TEXT = weight.iseid::TEXT
						LEFT JOIN (
							SELECT DISTINCT edw_vw_os_time_dim."year"
								,edw_vw_os_time_dim.mnth_id
								,edw_vw_os_time_dim.cal_date
							FROM edw_vw_os_time_dim
							) time_dim ON srv_det.createddate = time_dim.cal_date
						WHERE srv_det.quesclasscode = 1::CHARACTER VARYING::TEXT
							AND date_part(year, srv_det.createddate::TIMESTAMP WITHOUT TIME zone) >= (date_part(year, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 2::DOUBLE PRECISION)
						
						UNION ALL
						
						SELECT 'Merchandising_Response' AS dataset
							,srv_det.branchcode AS customerid
							,srv_det.slsperid AS salespersonid
							,srv_det.startdate AS mrch_resp_startdt
							,srv_det.enddate AS mrch_resp_enddt
							,NULL AS survey_enddate
							,NULL AS questiontext
							,NULL AS value
							,'true' AS mustcarryitem
							,CASE 
								WHEN srv_det.answerscore = 1
									THEN 'true'::CHARACTER VARYING
								ELSE 'false'::CHARACTER VARYING
								END AS answerscore
							,CASE 
								WHEN srv_det.answerscore <> 0
									OR srv_det.oos <> 0
									THEN 'true'::CHARACTER VARYING
								ELSE 'false'::CHARACTER VARYING
								END AS presence
							,CASE 
								WHEN srv_det.oos = 1
									THEN 'true'::CHARACTER VARYING
								ELSE ''::CHARACTER VARYING
								END AS outofstock
							,'OOS Compliance' AS kpi
							,srv_det.createddate AS scheduleddate
							,'completed' AS vst_status
							,time_dim."year" AS fisc_yr
							,time_dim.mnth_id::CHARACTER VARYING AS fisc_per
							,('(ISE) '::CHARACTER VARYING::TEXT || srv_det.name::TEXT)::CHARACTER VARYING AS firstname
							,'' AS lastname
							,(srv_det.branchcode::TEXT || ' - '::CHARACTER VARYING::TEXT || branch_master.branchname::TEXT)::CHARACTER VARYING AS customername
							,'Philippines' AS country
							,branch_master.area AS storereference
							,branch_master.channel AS storetype
							,branch_master.tradetype AS channel
							,branch_master.parentname AS salesgroup
							,product.franchise_nm AS prod_hier_l3
							,product.brand_nm AS prod_hier_l4
							,product.variant_nm AS prod_hier_l5
							,product.ph_ref_repvarputup_nm AS prod_hier_l6
							,NULL AS category
							,NULL AS segment
							,weight.weight AS kpi_chnl_wt
							,NULL AS mkt_share
							,NULL AS ques_desc
							,NULL AS "y/n_flag"
						FROM (
							SELECT edw_vw_ph_survey_details.branchcode
								,edw_vw_ph_survey_details.slsperid
								,edw_vw_ph_survey_details.custcode
								,edw_vw_ph_survey_details.iseid
								,edw_vw_ph_survey_details.ans_quesno
								,edw_vw_ph_survey_details.answerseq
								,edw_vw_ph_survey_details.answervalue
								,edw_vw_ph_survey_details.answerscore
								,edw_vw_ph_survey_details.oos
								,edw_vw_ph_survey_details.createddate
								,edw_vw_ph_survey_details.storeprioritization
								,edw_vw_ph_survey_details.name
								,edw_vw_ph_survey_details.putupid
								,edw_vw_ph_survey_details.putupdesc
								,edw_vw_ph_survey_details.brandid
								,edw_vw_ph_survey_details.brand
								,edw_vw_ph_survey_details.quesno
								,edw_vw_ph_survey_details.quesclasscode
								,edw_vw_ph_survey_details.quesclassdesc
								,edw_vw_ph_survey_details.totalscore
								,edw_vw_ph_survey_details.franchisecode
								,edw_vw_ph_survey_details.isedesc
								,edw_vw_ph_survey_details.startdate
								,edw_vw_ph_survey_details.enddate
							FROM edw_vw_ph_survey_details
							) srv_det
						LEFT JOIN (
							SELECT DISTINCT itg_mds_ph_product_hierarchy.ph_ref_repvarputup_cd
								,itg_mds_ph_product_hierarchy.franchise_nm
								,itg_mds_ph_product_hierarchy.brand_nm
								,itg_mds_ph_product_hierarchy.variant_nm
								,itg_mds_ph_product_hierarchy.ph_ref_repvarputup_nm
							FROM itg_mds_ph_product_hierarchy
							) product ON srv_det.putupid::TEXT = product.ph_ref_repvarputup_cd::TEXT
						LEFT JOIN (
							SELECT DISTINCT itg_ph_tbl_isebranchmaster.branchcode
								,itg_ph_tbl_isebranchmaster.branchname
								,itg_ph_tbl_isebranchmaster.area
								,itg_ph_tbl_isebranchmaster.channel
								,itg_ph_tbl_isebranchmaster.parentcode
								,itg_ph_tbl_isebranchmaster.parentname
								,itg_ph_tbl_isebranchmaster.tradetype
							FROM itg_ph_tbl_isebranchmaster
							) branch_master ON srv_det.custcode::TEXT = branch_master.parentcode::TEXT
							AND srv_det.branchcode::TEXT = branch_master.branchcode::TEXT
						LEFT JOIN (
							SELECT DISTINCT itg_mds_ph_ise_weights.iseid
								,itg_mds_ph_ise_weights.weight / 100::NUMERIC::NUMERIC(18, 0) AS weight
							FROM itg_mds_ph_ise_weights
							WHERE UPPER(trim(itg_mds_ph_ise_weights.kpi::TEXT)) = 'OSA'::CHARACTER VARYING::TEXT
							) weight ON srv_det.iseid::TEXT = weight.iseid::TEXT
						LEFT JOIN (
							SELECT DISTINCT edw_vw_os_time_dim."year"
								,edw_vw_os_time_dim.mnth_id
								,edw_vw_os_time_dim.cal_date
							FROM edw_vw_os_time_dim
							) time_dim ON srv_det.createddate = time_dim.cal_date
						WHERE srv_det.quesclasscode = 1::CHARACTER VARYING::TEXT
							AND date_part(year, srv_det.createddate::TIMESTAMP WITHOUT TIME zone) >= (date_part(year, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 2::DOUBLE PRECISION)
						),
union_2 as 				(
						
					SELECT 'Survey_Response' AS dataset
						,srv_det.branchcode AS customerid
						,srv_det.slsperid AS salespersonid
						,NULL AS mrch_resp_startdt
						,NULL AS mrch_resp_enddt
						,srv_det.enddate AS survey_enddate
						,srv_det.putupdesc AS questiontext
						,ques.sos_soa_value AS value
						,NULL AS mustcarryitem
						,srv_det.answerscore::CHARACTER VARYING AS answerscore
						,NULL AS presence
						,NULL AS outofstock
						,kpi.kpi_name AS kpi
						,srv_det.createddate AS scheduleddate
						,NULL AS vst_status
						,time_dim."year" AS fisc_yr
						,time_dim.mnth_id::CHARACTER VARYING AS fisc_per
						,('(ISE) '::CHARACTER VARYING::TEXT || srv_det.name::TEXT)::CHARACTER VARYING AS firstname
						,'' AS lastname
						,(srv_det.branchcode::TEXT || ' - '::CHARACTER VARYING::TEXT || branch_master.branchname::TEXT)::CHARACTER VARYING AS customername
						,'Philippines' AS country
						,branch_master.area AS storereference
						,branch_master.channel AS storetype
						,branch_master.tradetype AS channel
						,branch_master.parentname AS salesgroup
						,NULL AS prod_hier_l3
						,NULL AS prod_hier_l4
						,NULL AS prod_hier_l5
						,NULL AS prod_hier_l6
						,franchise.franchise_nm AS category
						,brand.brand_nm AS segment
						,weight.weight AS kpi_chnl_wt
						,1 AS mkt_share
						,ques.value AS ques_desc
						,NULL AS "y/n_flag"
					FROM (
						SELECT edw_vw_ph_survey_details.branchcode
							,edw_vw_ph_survey_details.slsperid
							,edw_vw_ph_survey_details.custcode
							,edw_vw_ph_survey_details.iseid
							,edw_vw_ph_survey_details.ans_quesno
							,edw_vw_ph_survey_details.answerseq
							,edw_vw_ph_survey_details.answervalue
							,edw_vw_ph_survey_details.answerscore
							,edw_vw_ph_survey_details.oos
							,edw_vw_ph_survey_details.createddate
							,edw_vw_ph_survey_details.storeprioritization
							,edw_vw_ph_survey_details.name
							,edw_vw_ph_survey_details.putupid
							,edw_vw_ph_survey_details.putupdesc
							,edw_vw_ph_survey_details.brandid
							,edw_vw_ph_survey_details.brand
							,edw_vw_ph_survey_details.quesno
							,edw_vw_ph_survey_details.quesclasscode
							,edw_vw_ph_survey_details.quesclassdesc
							,edw_vw_ph_survey_details.totalscore
							,edw_vw_ph_survey_details.franchisecode
							,edw_vw_ph_survey_details.isedesc
							,edw_vw_ph_survey_details.startdate
							,edw_vw_ph_survey_details.enddate
						FROM edw_vw_ph_survey_details
						) srv_det
					LEFT JOIN (
						SELECT DISTINCT itg_mds_ph_product_hierarchy.franchise_cd
							,itg_mds_ph_product_hierarchy.franchise_nm
						FROM itg_mds_ph_product_hierarchy
						) franchise ON srv_det.franchisecode::TEXT = franchise.franchise_cd::TEXT
					LEFT JOIN (
						SELECT DISTINCT itg_mds_ph_product_hierarchy.brand_cd
							,itg_mds_ph_product_hierarchy.brand_nm
						FROM itg_mds_ph_product_hierarchy
						) brand ON srv_det.brandid::TEXT = brand.brand_cd::TEXT
					LEFT JOIN (
						SELECT DISTINCT itg_ph_tbl_isebranchmaster.branchcode
							,itg_ph_tbl_isebranchmaster.branchname
							,itg_ph_tbl_isebranchmaster.area
							,itg_ph_tbl_isebranchmaster.channel
							,itg_ph_tbl_isebranchmaster.parentcode
							,itg_ph_tbl_isebranchmaster.parentname
							,itg_ph_tbl_isebranchmaster.tradetype
						FROM itg_ph_tbl_isebranchmaster
						) branch_master ON srv_det.custcode::TEXT = branch_master.parentcode::TEXT
						AND srv_det.branchcode::TEXT = branch_master.branchcode::TEXT
					LEFT JOIN (
						SELECT DISTINCT itg_mds_ph_ise_weights.iseid
							,UPPER(trim(itg_mds_ph_ise_weights.kpi::TEXT)) AS kpi
							,itg_mds_ph_ise_weights.weight / 100::NUMERIC::NUMERIC(18, 0) AS weight
						FROM itg_mds_ph_ise_weights
						) weight ON UPPER(trim(srv_det.quesclassdesc::TEXT)) like (('%'::CHARACTER VARYING::TEXT || weight.kpi) || '%'::CHARACTER VARYING::TEXT)
						AND srv_det.iseid::TEXT = weight.iseid::TEXT
					LEFT JOIN (
						SELECT DISTINCT edw_vw_os_time_dim."year"
							,edw_vw_os_time_dim.mnth_id
							,edw_vw_os_time_dim.cal_date
						FROM edw_vw_os_time_dim
						) time_dim ON srv_det.createddate = time_dim.cal_date
					LEFT JOIN (
						SELECT DISTINCT ph_kpi2data_mapping.data_type
							,ph_kpi2data_mapping.identifier
							,ph_kpi2data_mapping.kpi_name
						FROM ph_kpi2data_mapping
						WHERE ph_kpi2data_mapping.ctry::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
							AND ph_kpi2data_mapping.data_type::TEXT = 'KPI'::CHARACTER VARYING::TEXT
						) kpi ON srv_det.quesclasscode = kpi.identifier::TEXT
					LEFT JOIN (
						SELECT DISTINCT ph_kpi2data_mapping.data_type
							,trim(ph_kpi2data_mapping.identifier::TEXT) AS identifier
							,ph_kpi2data_mapping.kpi_name
							,ph_kpi2data_mapping.value
							,ph_kpi2data_mapping.sos_soa_value
						FROM ph_kpi2data_mapping
						WHERE ph_kpi2data_mapping.ctry::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
							AND ph_kpi2data_mapping.data_type::TEXT = 'Question'::CHARACTER VARYING::TEXT
						) ques ON kpi.kpi_name::TEXT = ques.kpi_name::TEXT
						AND srv_det.answerscore::CHARACTER VARYING::TEXT = ques.identifier
					WHERE srv_det.quesclasscode = '16'::CHARACTER VARYING::TEXT
						AND srv_det.createddate < '2020-04-27'::DATE
					) ,
union_3 as 			(
				
				SELECT 'Survey_Response' AS dataset
					,srv_det.branchcode AS customerid
					,srv_det.slsperid AS salespersonid
					,NULL AS mrch_resp_startdt
					,NULL AS mrch_resp_enddt
					,srv_det.enddate AS survey_enddate
					,srv_det.putupdesc AS questiontext
					,CASE 
						WHEN trim(UPPER(srv_det.answervalue::TEXT)) = 'NULL'::CHARACTER VARYING::TEXT
							OR trim(UPPER(srv_det.answervalue::TEXT)) = ''::CHARACTER VARYING::TEXT
							THEN '0'::CHARACTER VARYING
						ELSE COALESCE(srv_det.answervalue, '0'::CHARACTER VARYING)
						END AS value
					,NULL AS mustcarryitem
					,srv_det.answerscore::CHARACTER VARYING AS answerscore
					,NULL AS presence
					,NULL AS outofstock
					,kpi.kpi_name AS kpi
					,srv_det.createddate AS scheduleddate
					,NULL AS vst_status
					,time_dim."year" AS fisc_yr
					,time_dim.mnth_id::CHARACTER VARYING AS fisc_per
					,('(ISE) '::CHARACTER VARYING::TEXT || srv_det.name::TEXT)::CHARACTER VARYING AS firstname
					,'' AS lastname
					,(srv_det.branchcode::TEXT || ' - '::CHARACTER VARYING::TEXT || branch_master.branchname::TEXT)::CHARACTER VARYING AS customername
					,'Philippines' AS country
					,branch_master.area AS storereference
					,branch_master.channel AS storetype
					,branch_master.tradetype AS channel
					,branch_master.parentname AS salesgroup
					,NULL AS prod_hier_l3
					,NULL AS prod_hier_l4
					,NULL AS prod_hier_l5
					,NULL AS prod_hier_l6
					,franchise.franchise_nm AS category
					,brand.brand_nm AS segment
					,weight.weight AS kpi_chnl_wt
					,MARKET_SHARE.target AS mkt_share
					,CASE 
						WHEN (srv_det.answerseq % 2) = 1
							THEN 'numerator'::CHARACTER VARYING
						ELSE 'denominator'::CHARACTER VARYING
						END AS ques_desc
					,NULL AS "y/n_flag"
				FROM (
					SELECT edw_vw_ph_survey_details.branchcode
						,edw_vw_ph_survey_details.slsperid
						,edw_vw_ph_survey_details.custcode
						,edw_vw_ph_survey_details.iseid
						,edw_vw_ph_survey_details.ans_quesno
						,edw_vw_ph_survey_details.answerseq
						,edw_vw_ph_survey_details.answervalue
						,edw_vw_ph_survey_details.answerscore
						,edw_vw_ph_survey_details.oos
						,edw_vw_ph_survey_details.createddate
						,edw_vw_ph_survey_details.storeprioritization
						,edw_vw_ph_survey_details.name
						,edw_vw_ph_survey_details.putupid
						,edw_vw_ph_survey_details.putupdesc
						,edw_vw_ph_survey_details.brandid
						,edw_vw_ph_survey_details.brand
						,edw_vw_ph_survey_details.quesno
						,edw_vw_ph_survey_details.quesclasscode
						,edw_vw_ph_survey_details.quesclassdesc
						,edw_vw_ph_survey_details.totalscore
						,edw_vw_ph_survey_details.franchisecode
						,edw_vw_ph_survey_details.isedesc
						,edw_vw_ph_survey_details.startdate
						,edw_vw_ph_survey_details.enddate
					FROM edw_vw_ph_survey_details
					) srv_det
				LEFT JOIN (
					SELECT DISTINCT itg_mds_ph_product_hierarchy.franchise_cd
						,itg_mds_ph_product_hierarchy.franchise_nm
					FROM itg_mds_ph_product_hierarchy
					) franchise ON srv_det.franchisecode::TEXT = franchise.franchise_cd::TEXT
				LEFT JOIN (
					SELECT DISTINCT itg_mds_ph_product_hierarchy.brand_cd
						,itg_mds_ph_product_hierarchy.brand_nm
					FROM itg_mds_ph_product_hierarchy
					) brand ON srv_det.brandid::TEXT = brand.brand_cd::TEXT
				LEFT JOIN (
					SELECT DISTINCT itg_ph_tbl_isebranchmaster.branchcode
						,itg_ph_tbl_isebranchmaster.branchname
						,itg_ph_tbl_isebranchmaster.area
						,itg_ph_tbl_isebranchmaster.channel
						,itg_ph_tbl_isebranchmaster.parentcode
						,itg_ph_tbl_isebranchmaster.parentname
						,itg_ph_tbl_isebranchmaster.tradetype
					FROM itg_ph_tbl_isebranchmaster
					) branch_master ON srv_det.custcode::TEXT = branch_master.parentcode::TEXT
					AND srv_det.branchcode::TEXT = branch_master.branchcode::TEXT
				LEFT JOIN (
					SELECT DISTINCT itg_mds_ph_ise_weights.iseid
						,UPPER(trim(itg_mds_ph_ise_weights.kpi::TEXT)) AS kpi
						,itg_mds_ph_ise_weights.weight / 100::NUMERIC::NUMERIC(18, 0) AS weight
					FROM itg_mds_ph_ise_weights
					) weight ON UPPER(trim(srv_det.quesclassdesc::TEXT)) like (('%'::CHARACTER VARYING::TEXT || weight.kpi) || '%'::CHARACTER VARYING::TEXT)
					AND srv_det.iseid::TEXT = weight.iseid::TEXT
				LEFT JOIN (
					SELECT DISTINCT edw_vw_os_time_dim."year"
						,edw_vw_os_time_dim.mnth_id
						,edw_vw_os_time_dim.cal_date
					FROM edw_vw_os_time_dim
					) time_dim ON srv_det.createddate = time_dim.cal_date
				LEFT JOIN (
					SELECT DISTINCT ph_kpi2data_mapping.data_type
						,ph_kpi2data_mapping.identifier
						,ph_kpi2data_mapping.kpi_name
					FROM ph_kpi2data_mapping
					WHERE ph_kpi2data_mapping.ctry::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
						AND ph_kpi2data_mapping.data_type::TEXT = 'KPI'::CHARACTER VARYING::TEXT
					) kpi ON srv_det.quesclasscode = kpi.identifier::TEXT
				LEFT JOIN (
					SELECT DISTINCT itg_mds_ph_ise_sos_targets.cal_year
						,COALESCE(CASE 
								WHEN trim(itg_mds_ph_ise_sos_targets.brand_id::TEXT) = ''::CHARACTER VARYING::TEXT
									THEN NULL::CHARACTER VARYING::TEXT
								ELSE trim(itg_mds_ph_ise_sos_targets.brand_id::TEXT)
								END, 'X'::CHARACTER VARYING::TEXT) AS brand_id
						,ITG_MDS_PH_ISE_SOS_TARGETS.target
					FROM itg_mds_ph_ise_sos_targets
					) market_share ON COALESCE(CASE 
							WHEN trim(srv_det.brandid::TEXT) = ''::CHARACTER VARYING::TEXT
								THEN NULL::CHARACTER VARYING::TEXT
							ELSE trim(srv_det.brandid::TEXT)
							END, 'X'::CHARACTER VARYING::TEXT) = market_share.brand_id
					AND date_part(year, srv_det.createddate::TIMESTAMP WITHOUT TIME zone)::CHARACTER VARYING::TEXT = market_share.cal_year::TEXT
				WHERE srv_det.quesclasscode = '16'::CHARACTER VARYING::TEXT
					AND srv_det.createddate >= '2020-04-27'::DATE
				) ,				
union_4 as (			
			
			SELECT 'Survey_Response' AS dataset
				,srv_det.branchcode AS customerid
				,srv_det.slsperid AS salespersonid
				,NULL AS mrch_resp_startdt
				,NULL AS mrch_resp_enddt
				,srv_det.enddate AS survey_enddate
				,srv_det.putupdesc AS questiontext
				,NULL AS value
				,NULL AS mustcarryitem
				,CASE 
					WHEN srv_det.answerscore = 1
						THEN 'true'::CHARACTER VARYING
					ELSE 'false'::CHARACTER VARYING
					END AS answerscore
				,NULL AS presence
				,NULL AS outofstock
				,kpi.kpi_name AS kpi
				,srv_det.createddate AS scheduleddate
				,NULL AS vst_status
				,time_dim."year" AS fisc_yr
				,time_dim.mnth_id::CHARACTER VARYING AS fisc_per
				,('(ISE) '::CHARACTER VARYING::TEXT || srv_det.name::TEXT)::CHARACTER VARYING AS firstname
				,'' AS lastname
				,(srv_det.branchcode::TEXT || ' - '::CHARACTER VARYING::TEXT || branch_master.branchname::TEXT)::CHARACTER VARYING AS customername
				,'Philippines' AS country
				,branch_master.area AS storereference
				,branch_master.channel AS storetype
				,branch_master.tradetype AS channel
				,branch_master.parentname AS salesgroup
				,NULL AS prod_hier_l3
				,NULL AS prod_hier_l4
				,NULL AS prod_hier_l5
				,NULL AS prod_hier_l6
				,product.franchise_nm AS category
				,NULL AS segment
				,weight.weight AS kpi_chnl_wt
				,NULL AS mkt_share
				,NULL AS ques_desc
				,flag.value AS "y/n_flag"
			FROM (
				SELECT edw_vw_ph_survey_details.branchcode
					,edw_vw_ph_survey_details.slsperid
					,edw_vw_ph_survey_details.custcode
					,edw_vw_ph_survey_details.iseid
					,edw_vw_ph_survey_details.ans_quesno
					,edw_vw_ph_survey_details.answerseq
					,edw_vw_ph_survey_details.answervalue
					,edw_vw_ph_survey_details.answerscore
					,edw_vw_ph_survey_details.oos
					,edw_vw_ph_survey_details.createddate
					,edw_vw_ph_survey_details.storeprioritization
					,edw_vw_ph_survey_details.name
					,edw_vw_ph_survey_details.putupid
					,edw_vw_ph_survey_details.putupdesc
					,edw_vw_ph_survey_details.brandid
					,edw_vw_ph_survey_details.brand
					,edw_vw_ph_survey_details.quesno
					,edw_vw_ph_survey_details.quesclasscode
					,edw_vw_ph_survey_details.quesclassdesc
					,edw_vw_ph_survey_details.totalscore
					,edw_vw_ph_survey_details.franchisecode
					,edw_vw_ph_survey_details.isedesc
					,edw_vw_ph_survey_details.startdate
					,edw_vw_ph_survey_details.enddate
				FROM edw_vw_ph_survey_details
				) srv_det
			LEFT JOIN (
				SELECT DISTINCT itg_mds_ph_product_hierarchy.franchise_cd
					,itg_mds_ph_product_hierarchy.franchise_nm
				FROM itg_mds_ph_product_hierarchy
				) product ON srv_det.franchisecode::TEXT = product.franchise_cd::TEXT
			LEFT JOIN (
				SELECT DISTINCT itg_ph_tbl_isebranchmaster.branchcode
					,itg_ph_tbl_isebranchmaster.branchname
					,itg_ph_tbl_isebranchmaster.area
					,itg_ph_tbl_isebranchmaster.channel
					,itg_ph_tbl_isebranchmaster.parentcode
					,itg_ph_tbl_isebranchmaster.parentname
					,itg_ph_tbl_isebranchmaster.tradetype
				FROM itg_ph_tbl_isebranchmaster
				) branch_master ON srv_det.custcode::TEXT = branch_master.parentcode::TEXT
				AND srv_det.branchcode::TEXT = branch_master.branchcode::TEXT
			LEFT JOIN (
				SELECT DISTINCT itg_mds_ph_ise_weights.iseid
					,UPPER(trim(itg_mds_ph_ise_weights.kpi::TEXT)) AS kpi
					,itg_mds_ph_ise_weights.weight / 100::NUMERIC::NUMERIC(18, 0) AS weight
				FROM itg_mds_ph_ise_weights
				) weight ON UPPER(trim(srv_det.quesclassdesc::TEXT)) like (('%'::CHARACTER VARYING::TEXT || weight.kpi) || '%'::CHARACTER VARYING::TEXT)
				AND srv_det.iseid::TEXT = weight.iseid::TEXT
			LEFT JOIN (
				SELECT DISTINCT edw_vw_os_time_dim."year"
					,edw_vw_os_time_dim.mnth_id
					,edw_vw_os_time_dim.cal_date
				FROM edw_vw_os_time_dim
				) time_dim ON srv_det.createddate = time_dim.cal_date
			LEFT JOIN (
				SELECT DISTINCT ph_kpi2data_mapping.data_type
					,ph_kpi2data_mapping.identifier
					,ph_kpi2data_mapping.kpi_name
				FROM ph_kpi2data_mapping
				WHERE ph_kpi2data_mapping.ctry::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
					AND ph_kpi2data_mapping.data_type::TEXT = 'KPI'::CHARACTER VARYING::TEXT
				) kpi ON srv_det.quesclasscode = kpi.identifier::TEXT
			LEFT JOIN (
				SELECT DISTINCT ph_kpi2data_mapping.data_type
					,trim(ph_kpi2data_mapping.identifier::TEXT) AS identifier
					,ph_kpi2data_mapping.kpi_name
					,ph_kpi2data_mapping.value
				FROM ph_kpi2data_mapping
				WHERE ph_kpi2data_mapping.ctry::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
					AND ph_kpi2data_mapping.data_type::TEXT = 'Yes/No Flag'::CHARACTER VARYING::TEXT
				) flag ON kpi.kpi_name::TEXT = flag.kpi_name::TEXT
				AND srv_det.answerscore::CHARACTER VARYING::TEXT = flag.identifier
			WHERE srv_det.quesclasscode <> '1'::CHARACTER VARYING::TEXT
				AND srv_det.quesclasscode <> '16'::CHARACTER VARYING::TEXT
				AND srv_det.quesclasscode <> '12'::CHARACTER VARYING::TEXT
				AND date_part(year, srv_det.createddate::TIMESTAMP WITHOUT TIME zone) >= (date_part(year, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 2::DOUBLE PRECISION)
				AND kpi.kpi_name <> 'Promo Compliance'
			)  ,	
union_5 as 	(		
		SELECT 'Survey_Response' AS dataset
			,srv_det.branchcode AS customerid
			,srv_det.slsperid AS salespersonid
			,NULL AS mrch_resp_startdt
			,NULL AS mrch_resp_enddt
			,srv_det.enddate AS survey_enddate
			,srv_det.putupdesc AS questiontext
			,NULL AS value
			,NULL AS mustcarryitem
			,CASE 
				WHEN srv_det.answerscore = 1
					THEN 'true'::CHARACTER VARYING
				ELSE 'false'::CHARACTER VARYING
				END AS answerscore
			,NULL AS presence
			,NULL AS outofstock
			,kpi.kpi_name AS kpi
			,srv_det.createddate AS scheduleddate
			,NULL AS vst_status
			,time_dim."year" AS fisc_yr
			,time_dim.mnth_id::CHARACTER VARYING AS fisc_per
			,('(ISE) '::CHARACTER VARYING::TEXT || srv_det.name::TEXT)::CHARACTER VARYING AS firstname
			,'' AS lastname
			,(srv_det.branchcode::TEXT || ' - '::CHARACTER VARYING::TEXT || branch_master.branchname::TEXT)::CHARACTER VARYING AS customername
			,'Philippines' AS country
			,branch_master.area AS storereference
			,branch_master.channel AS storetype
			,branch_master.tradetype AS channel
			,branch_master.parentname AS salesgroup
			,NULL AS prod_hier_l3
			,NULL AS prod_hier_l4
			,NULL AS prod_hier_l5
			,NULL AS prod_hier_l6
			,product.franchise_nm AS category
			,NULL AS segment
			,weight.weight AS kpi_chnl_wt
			,NULL AS mkt_share
			,NULL AS ques_desc
			,flag.value AS "y/n_flag"
		FROM (
			SELECT edw_vw_ph_survey_details.branchcode
				,edw_vw_ph_survey_details.slsperid
				,edw_vw_ph_survey_details.custcode
				,edw_vw_ph_survey_details.iseid
				,edw_vw_ph_survey_details.ans_quesno
				,edw_vw_ph_survey_details.answerseq
				,edw_vw_ph_survey_details.answervalue
				,edw_vw_ph_survey_details.answerscore
				,edw_vw_ph_survey_details.oos
				,edw_vw_ph_survey_details.createddate
				,edw_vw_ph_survey_details.storeprioritization
				,edw_vw_ph_survey_details.name
				,edw_vw_ph_survey_details.putupid
				,edw_vw_ph_survey_details.putupdesc
				,edw_vw_ph_survey_details.brandid
				,edw_vw_ph_survey_details.brand
				,edw_vw_ph_survey_details.quesno
				,edw_vw_ph_survey_details.quesclasscode
				,edw_vw_ph_survey_details.quesclassdesc
				,edw_vw_ph_survey_details.totalscore
				,edw_vw_ph_survey_details.franchisecode
				,edw_vw_ph_survey_details.isedesc
				,edw_vw_ph_survey_details.startdate
				,edw_vw_ph_survey_details.enddate
				,
   CASE
    WHEN lower(edw_vw_ph_survey_details.answernotes) REGEXP '( n\\.a| n/a| na| n a|\\.n\\.a|\\.n/a|\\.na|\\.n a|,n\\.a|,n/a|,na|,n a|-n\\.a|-n/a|-na|-n a|\\)n\\.a|\\)n/a|\\)na|\\)n a)' THEN 'na'
    WHEN lower(edw_vw_ph_survey_details.answernotes) REGEXP '(n\\.a |n/a |na |n a |n\\.a\\.|n/a\\.|na\\.|n a\\.|n\\.a,|n/a,|na,|n a,|n\\.a-|n/a-|na-|n a-|n\\.a\\(|n/a\\(|na\\(|n a\\()' THEN 'na'
    WHEN lower(edw_vw_ph_survey_details.answernotes) REGEXP 'n\\.a|n/a|na|n a' THEN 'na'
    WHEN lower(edw_vw_ph_survey_details.answernotes) REGEXP ' n/a |not applicable' THEN 'na'
    ELSE edw_vw_ph_survey_details.answernotes
END as answernotes
			FROM edw_vw_ph_survey_details
			) srv_det
		LEFT JOIN (
			SELECT DISTINCT itg_mds_ph_product_hierarchy.franchise_cd
				,itg_mds_ph_product_hierarchy.franchise_nm
			FROM itg_mds_ph_product_hierarchy
			) product ON rtrim(srv_det.franchisecode)::TEXT = rtrim(product.franchise_cd)::TEXT
		LEFT JOIN (
			SELECT DISTINCT itg_ph_tbl_isebranchmaster.branchcode
				,itg_ph_tbl_isebranchmaster.branchname
				,itg_ph_tbl_isebranchmaster.area
				,itg_ph_tbl_isebranchmaster.channel
				,itg_ph_tbl_isebranchmaster.parentcode
				,itg_ph_tbl_isebranchmaster.parentname
				,itg_ph_tbl_isebranchmaster.tradetype
			FROM itg_ph_tbl_isebranchmaster
			) branch_master ON rtrim(srv_det.custcode)::TEXT = rtrim(branch_master.parentcode)::TEXT
			AND rtrim(srv_det.branchcode)::TEXT = rtrim(branch_master.branchcode)::TEXT
		LEFT JOIN (
			SELECT DISTINCT itg_mds_ph_ise_weights.iseid
				,UPPER(trim(itg_mds_ph_ise_weights.kpi::TEXT)) AS kpi
				,itg_mds_ph_ise_weights.weight / 100::NUMERIC::NUMERIC(18, 0) AS weight
			FROM itg_mds_ph_ise_weights
			) weight ON UPPER(trim(srv_det.quesclassdesc::TEXT)) like (('%'::CHARACTER VARYING::TEXT || weight.kpi) || '%'::CHARACTER VARYING::TEXT)
			AND rtrim(srv_det.iseid)::TEXT = rtrim(weight.iseid)::TEXT
		LEFT JOIN (
			SELECT DISTINCT edw_vw_os_time_dim."year"
				,edw_vw_os_time_dim.mnth_id
				,edw_vw_os_time_dim.cal_date
			FROM edw_vw_os_time_dim
			) time_dim ON rtrim(srv_det.createddate) = rtrim(time_dim.cal_date)
		LEFT JOIN (
			SELECT DISTINCT ph_kpi2data_mapping.data_type
				,ph_kpi2data_mapping.identifier
				,ph_kpi2data_mapping.kpi_name
			FROM ph_kpi2data_mapping
			WHERE rtrim(ph_kpi2data_mapping.ctry)::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
				AND rtrim(ph_kpi2data_mapping.data_type)::TEXT = 'KPI'::CHARACTER VARYING::TEXT
			) kpi ON rtrim(srv_det.quesclasscode) = rtrim(kpi.identifier)::TEXT
		LEFT JOIN (
			SELECT DISTINCT ph_kpi2data_mapping.data_type
				,trim(ph_kpi2data_mapping.identifier::TEXT) AS identifier
				,ph_kpi2data_mapping.kpi_name
				,ph_kpi2data_mapping.value
			FROM ph_kpi2data_mapping
			WHERE rtrim(ph_kpi2data_mapping.ctry)::TEXT = 'Philippines'::CHARACTER VARYING::TEXT
				AND rtrim(ph_kpi2data_mapping.data_type)::TEXT = 'Yes/No Flag'::CHARACTER VARYING::TEXT
			) flag ON rtrim(kpi.kpi_name)::TEXT = rtrim(flag.kpi_name)::TEXT
			AND rtrim(srv_det.answerscore)::CHARACTER VARYING::TEXT = rtrim(flag.identifier)
		WHERE rtrim(srv_det.quesclasscode) = '12'::CHARACTER VARYING::TEXT
			AND date_part(year, srv_det.createddate::TIMESTAMP WITHOUT TIME zone) >= (date_part(year, convert_timezone('UTC',current_timestamp())::timestamp_ntz(9)) - 2::DOUBLE PRECISION)
			AND (
				(lower(srv_det.answernotes) != 'na')
				OR lower(srv_det.answernotes) IS NULL
				)
		),		
union_6 as (
SELECT 'Merchandising_Response' AS dataset,
        (ippmo.ret_nm_prefix::TEXT || '-'::CHARACTER VARYING::TEXT || ippmo.branch_code::TEXT)::CHARACTER VARYING AS customerid,
        NULL AS salespersonid,
        NULL AS mrch_resp_startdt,
        NULL AS mrch_resp_enddt,
        NULL AS survey_enddate,
        NULL AS questiontext,
        NULL AS value,
        'true' AS mustcarryitem,
        NULL AS answerscore,
        CASE 
                WHEN ippmo.osa_flag::TEXT = '1'::CHARACTER VARYING::TEXT
                        OR ippmo.osa_flag::TEXT = '0'::CHARACTER VARYING::TEXT
                        THEN 'true'::CHARACTER VARYING
                ELSE 'false'::CHARACTER VARYING
                END AS presence,
        CASE 
                WHEN ippmo.osa_flag::TEXT = '0'::CHARACTER VARYING::TEXT
                        THEN 'true'::CHARACTER VARYING
                ELSE ''::CHARACTER VARYING
                END AS outofstock,
        'MSL Compliance' AS kpi,
        ippmo.osa_check_date AS scheduleddate,
        'completed' AS vst_status,
        evotd."year" AS fisc_yr,
        evotd.mnth_id::CHARACTER VARYING AS fisc_per,
        ('(Non-ISE) '::CHARACTER VARYING::TEXT || ippmo.team_leader::TEXT)::CHARACTER VARYING AS firstname,
        '' AS lastname,
        (ippmo.ret_nm_prefix::TEXT || '-'::CHARACTER VARYING::TEXT || ippmo.branch_code::TEXT || ' - '::CHARACTER VARYING::TEXT || ippmo.brnch_nm::TEXT)::CHARACTER VARYING AS customername,
        'Philippines' AS country,
        cust.parentcustomername AS storereference,
        ippmo.channeldesc AS storetype,
        cust.rpt_grp_1_desc AS channel,
        (cust.parentcustomername::TEXT || '-'::CHARACTER VARYING::TEXT || ippmo.ret_nm_prefix::TEXT)::CHARACTER VARYING AS salesgroup,
        implp.scard_franchise_desc AS prod_hier_l3,
        implp.scard_brand_desc AS prod_hier_l4,
        implp.scard_varient_desc AS prod_hier_l5,
        implp.scard_put_up_desc AS prod_hier_l6,
        NULL AS category,
        NULL AS segment,
        ippmo.weight AS kpi_chnl_wt,
        NULL AS mkt_share,
        NULL AS ques_desc,
        NULL AS "y/n_flag"
FROM (
        SELECT a.channeldesc,
                b.ret_nm_prefix,
                b.retailer_name,
                b.team_leader,
                b.branch_code,
                b.brnch_nm,
                b.scard_franchise_cd,
                b.scard_franchise_desc,
                b.scard_brand_cd,
                b.scard_brand_desc,
                a.brand,
                b.scard_put_up_cd,
                b.scard_put_up_desc,
                b.barcode,
                b.sku_code,
                b.item_description,
                b.osa_check_date,
                a.msl_flag,
                b.osa_flag,
                b.weight
        FROM (
                SELECT DISTINCT b.startdate,
                        b.enddate,
                        a.iseid,
                        a.quesno,
                        a.quesclassdesc,
                        c.answerseq,
                        b.channelcode,
                        b.channeldesc,
                        b.isedesc,
                        a.franchisecode,
                        a.franchisedesc,
                        c.brandid,
                        c.brand,
                        c.putupid,
                        c.putupdesc,
                        a.product_flag AS msl_flag
                FROM itg_ph_tbl_surveyisequestion a
                LEFT JOIN itg_ph_tbl_surveyisehdr b ON rtrim(a.iseid)::TEXT = rtrim(b.iseid)::TEXT
                JOIN itg_ph_tbl_surveychoices c ON rtrim(b.iseid)::TEXT = rtrim(c.iseid)::TEXT
                        AND a.quesno = c.quesno
                JOIN itg_mds_ph_product_hierarchy e ON rtrim(e.franchise_cd)::TEXT = rtrim(a.franchisecode)::TEXT
                        AND rtrim(e.brand_cd)::TEXT = rtrim(c.brandid)::TEXT
                        AND rtrim(e.ph_ref_repvarputup_cd)::TEXT = rtrim(c.putupid)::TEXT
                JOIN itg_mds_ph_lav_product i ON rtrim(i.scard_put_up_cd)::TEXT = rtrim(e.ph_ref_repvarputup_cd)::TEXT
                        AND rtrim(e.brand_cd)::TEXT = rtrim(i.scard_brand_cd)::TEXT
                        AND rtrim(e.franchise_cd)::TEXT = rtrim(i.scard_franchise_cd)::TEXT
                WHERE upper(rtrim(a.quesclassdesc)::TEXT) = 'MUST CARRY LIST'::CHARACTER VARYING::TEXT
                        AND a.product_flag = 1
                        AND upper(rtrim(i.active)::TEXT) = 'Y'::CHARACTER VARYING::TEXT
                ) a
        LEFT JOIN (
                SELECT a.team_leader,
                        a.ret_nm_prefix,
                        a.retailer_name,
                        a.branch_code,
                        c.brnch_nm,
                        c.store_mtrx,
                        b.scard_franchise_cd,
                        b.scard_franchise_desc,
                        b.scard_brand_cd,
                        b.scard_brand_desc,
                        b.scard_put_up_cd,
                        b.scard_put_up_desc,
                        a.barcode,
                        a.sku_code,
                        a.item_description,
                        a.osa_check_date,
                        a.osa_flag,
                        (
                                SELECT DISTINCT itg_ph_non_ise_weights.weight
                                FROM itg_ph_non_ise_weights
                                WHERE upper(itg_ph_non_ise_weights.kpi::TEXT) = 'MSL COMPLIANCE'::CHARACTER VARYING::TEXT
                                ) AS weight
                FROM itg_ph_non_ise_msl_osa a
                LEFT JOIN (
                        SELECT DISTINCT itg_mds_ph_lav_product.item_cd,
                                itg_mds_ph_lav_product.scard_franchise_cd,
                                itg_mds_ph_lav_product.scard_franchise_desc,
                                itg_mds_ph_lav_product.scard_brand_cd,
                                itg_mds_ph_lav_product.scard_brand_desc,
                                itg_mds_ph_lav_product.scard_put_up_cd,
                                itg_mds_ph_lav_product.scard_put_up_desc
                        FROM itg_mds_ph_lav_product
                        WHERE upper(itg_mds_ph_lav_product.active::TEXT) = 'Y'::CHARACTER VARYING::TEXT
                        ) b ON ltrim(rtrim(a.sku_code)::TEXT, '0'::CHARACTER VARYING::TEXT) = ltrim(rtrim(b.item_cd)::TEXT, '0'::CHARACTER VARYING::TEXT)
                LEFT JOIN (
                        SELECT DISTINCT ltrim(itg_mds_ph_pos_customers.brnch_cd::TEXT, '0'::CHARACTER VARYING::TEXT) AS brnch_cd,
                                upper(itg_mds_ph_pos_customers.cust_cd::TEXT) AS cust_cd,
                                itg_mds_ph_pos_customers.brnch_nm,
                                itg_mds_ph_pos_customers.store_mtrx
                        FROM itg_mds_ph_pos_customers
                        WHERE upper(rtrim(itg_mds_ph_pos_customers.active)::TEXT) = 'Y'::CHARACTER VARYING::TEXT
                        ) c ON upper(rtrim(c.brnch_cd)) = upper(rtrim(a.branch_code)::TEXT)
                        AND upper(rtrim(c.cust_cd)) = upper(rtrim(a.ret_nm_prefix)::TEXT)
                ) b ON upper(rtrim(b.store_mtrx)::TEXT) = upper(rtrim(a.channeldesc)::TEXT)
                AND rtrim(a.franchisecode)::TEXT = rtrim(b.scard_franchise_cd)::TEXT
                AND rtrim(a.brandid)::TEXT = rtrim(b.scard_brand_cd)::TEXT
                AND rtrim(a.putupid)::TEXT = rtrim(b.scard_put_up_cd)::TEXT
                AND b.osa_check_date >= a.startdate
                AND b.osa_check_date <= a.enddate
        ) ippmo
LEFT JOIN (
        SELECT DISTINCT itg_mds_ph_lav_product.item_cd,
                itg_mds_ph_lav_product.scard_franchise_desc,
                itg_mds_ph_lav_product.scard_brand_desc,
                itg_mds_ph_lav_product.scard_varient_desc,
                itg_mds_ph_lav_product.scard_put_up_desc
        FROM itg_mds_ph_lav_product
        WHERE upper(itg_mds_ph_lav_product.active::TEXT) = 'Y'::CHARACTER VARYING::TEXT
        ) implp ON ltrim(rtrim(implp.item_cd)::TEXT, '0'::CHARACTER VARYING::TEXT) = ltrim(rtrim(ippmo.sku_code)::TEXT, '0'::CHARACTER VARYING::TEXT)
LEFT JOIN (
        SELECT DISTINCT itg_ph_ps_retailer_soldto_map.retailer_name,
                itg_ph_ps_retailer_soldto_map.sold_to
        FROM itg_ph_ps_retailer_soldto_map
        ) ipprsm ON upper(trim(ipprsm.retailer_name::TEXT)) = upper(trim(ippmo.retailer_name::TEXT))
LEFT JOIN (
        SELECT impip.soldtocode,
                impip.parentcustomername,
                imprpc.rpt_grp_12_desc,
                imprpc.rpt_grp_1_desc
        FROM (
                SELECT DISTINCT itg_mds_ph_ise_parent.soldtocode,
                        itg_mds_ph_ise_parent.parentcustomercode,
                        itg_mds_ph_ise_parent.parentcustomername
                FROM itg_mds_ph_ise_parent
                ) impip
        LEFT JOIN (
                SELECT DISTINCT itg_mds_ph_ref_parent_customer.parent_cust_cd,
                        itg_mds_ph_ref_parent_customer.rpt_grp_12_desc,
                        itg_mds_ph_ref_parent_customer.rpt_grp_1_desc
                FROM itg_mds_ph_ref_parent_customer
                WHERE upper(rtrim(itg_mds_ph_ref_parent_customer.active)::TEXT) = 'Y'::CHARACTER VARYING::TEXT
                ) imprpc ON rtrim(impip.parentcustomercode)::TEXT = rtrim(imprpc.parent_cust_cd)::TEXT
        ) cust ON rtrim(ipprsm.sold_to)::TEXT = rtrim(cust.soldtocode)::TEXT
LEFT JOIN (
        SELECT DISTINCT edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.cal_date
        FROM edw_vw_os_time_dim
        ) evotd ON ippmo.osa_check_date = evotd.cal_date
WHERE DATE_PART(year, ippmo.osa_check_date::TIMESTAMP without TIME zone) >= (DATE_PART(year, current_timestamp()) - 2::DOUBLE PRECISION) 
),
union_7 as (
  SELECT 
    'Merchandising_Response' AS dataset, 
    (
      ippmo.ret_nm_prefix :: TEXT || '-' :: CHARACTER VARYING :: TEXT || ippmo.branch_code :: TEXT
    ):: CHARACTER VARYING AS customerid, 
    NULL AS salespersonid, 
    NULL AS mrch_resp_startdt, 
    NULL AS mrch_resp_enddt, 
    NULL AS survey_enddate, 
    NULL AS questiontext, 
    NULL AS value, 
    'true' AS mustcarryitem, 
    NULL AS answerscore, 
    'true' AS presence, 
    CASE WHEN ippmo.osa_flag :: TEXT = '0' :: CHARACTER VARYING :: TEXT THEN 'true' :: CHARACTER VARYING ELSE '' :: CHARACTER VARYING END AS outofstock, 
    'OOS Compliance' AS kpi, 
    ippmo.osa_check_date AS scheduleddate, 
    'completed' AS vst_status, 
    evotd."year" AS fisc_yr, 
    evotd.mnth_id :: CHARACTER VARYING AS fisc_per, 
    (
      '(Non-ISE) ' :: CHARACTER VARYING :: TEXT || ippmo.team_leader :: TEXT
    ):: CHARACTER VARYING AS firstname, 
    '' AS lastname, 
    (
      ippmo.ret_nm_prefix :: TEXT || '-' :: CHARACTER VARYING :: TEXT || ippmo.branch_code :: TEXT || ' - ' :: CHARACTER VARYING :: TEXT || ippmo.brnch_nm :: TEXT
    ):: CHARACTER VARYING AS customername, 
    'Philippines' AS country, 
    cust.parentcustomername AS storereference, 
    ippmo.channeldesc AS storetype, 
    cust.rpt_grp_1_desc AS channel, 
    (
      cust.parentcustomername :: TEXT || '-' :: CHARACTER VARYING :: TEXT || ippmo.ret_nm_prefix :: TEXT
    ):: CHARACTER VARYING AS salesgroup, 
    implp.scard_franchise_desc AS prod_hier_l3, 
    implp.scard_brand_desc AS prod_hier_l4, 
    implp.scard_varient_desc AS prod_hier_l5, 
    implp.scard_put_up_desc AS prod_hier_l6, 
    NULL AS category, 
    NULL AS segment, 
    ippmo.weight AS kpi_chnl_wt, 
    NULL AS mkt_share, 
    NULL AS ques_desc, 
    NULL AS "y/n_flag" 
  FROM 
    (
      SELECT 
        a.channeldesc, 
        b.ret_nm_prefix, 
        b.retailer_name, 
        b.team_leader, 
        b.branch_code, 
        b.brnch_nm, 
        b.scard_franchise_cd, 
        b.scard_franchise_desc, 
        b.scard_brand_cd, 
        b.scard_brand_desc, 
        a.brand, 
        b.scard_put_up_cd, 
        b.scard_put_up_desc, 
        b.barcode, 
        b.sku_code, 
        b.item_description, 
        b.osa_check_date, 
        a.msl_flag, 
        b.osa_flag, 
        b.weight 
      FROM 
        (
          SELECT 
            DISTINCT b.startdate, 
            b.enddate, 
            a.iseid, 
            a.quesno, 
            a.quesclassdesc, 
            c.answerseq, 
            b.channelcode, 
            b.channeldesc, 
            b.isedesc, 
            a.franchisecode, 
            a.franchisedesc, 
            c.brandid, 
            c.brand, 
            c.putupid, 
            c.putupdesc, 
            a.product_flag AS msl_flag 
          FROM 
            itg_ph_tbl_surveyisequestion a 
            LEFT JOIN itg_ph_tbl_surveyisehdr b ON rtrim(a.iseid):: TEXT = rtrim(b.iseid):: TEXT 
            JOIN itg_ph_tbl_surveychoices c ON rtrim(b.iseid):: TEXT = rtrim(c.iseid):: TEXT 
            AND rtrim(a.quesno) = rtrim(c.quesno) 
            JOIN itg_mds_ph_product_hierarchy e ON rtrim(e.franchise_cd):: TEXT = rtrim(a.franchisecode):: TEXT 
            AND rtrim(e.brand_cd):: TEXT = rtrim(c.brandid):: TEXT 
            AND rtrim(e.ph_ref_repvarputup_cd):: TEXT = rtrim(c.putupid):: TEXT 
            JOIN itg_mds_ph_lav_product i ON rtrim(i.scard_put_up_cd):: TEXT = rtrim(e.ph_ref_repvarputup_cd):: TEXT 
            AND rtrim(e.brand_cd):: TEXT = rtrim(i.scard_brand_cd):: TEXT 
            AND rtrim(e.franchise_cd):: TEXT = rtrim(i.scard_franchise_cd):: TEXT 
          WHERE 
            upper(
              rtrim(a.quesclassdesc):: TEXT
            ) = 'MUST CARRY LIST' :: CHARACTER VARYING :: TEXT 
            AND a.product_flag = 1 
            AND upper(
              rtrim(i.active):: TEXT
            ) = 'Y' :: CHARACTER VARYING :: TEXT
        ) a 
        LEFT JOIN (
          SELECT 
            a.team_leader, 
            a.ret_nm_prefix, 
            a.retailer_name, 
            a.branch_code, 
            c.brnch_nm, 
            c.store_mtrx, 
            b.scard_franchise_cd, 
            b.scard_franchise_desc, 
            b.scard_brand_cd, 
            b.scard_brand_desc, 
            b.scard_put_up_cd, 
            b.scard_put_up_desc, 
            a.barcode, 
            a.sku_code, 
            a.item_description, 
            a.osa_check_date, 
            a.osa_flag, 
            (
              SELECT 
                DISTINCT itg_ph_non_ise_weights.weight 
              FROM 
                itg_ph_non_ise_weights 
              WHERE 
                upper(
                  itg_ph_non_ise_weights.kpi :: TEXT
                ) = 'OOS COMPLIANCE' :: CHARACTER VARYING :: TEXT
            ) AS weight 
          FROM 
            itg_ph_non_ise_msl_osa a 
            LEFT JOIN (
              SELECT 
                DISTINCT itg_mds_ph_lav_product.item_cd, 
                itg_mds_ph_lav_product.scard_franchise_cd, 
                itg_mds_ph_lav_product.scard_franchise_desc, 
                itg_mds_ph_lav_product.scard_brand_cd, 
                itg_mds_ph_lav_product.scard_brand_desc, 
                itg_mds_ph_lav_product.scard_put_up_cd, 
                itg_mds_ph_lav_product.scard_put_up_desc 
              FROM 
                itg_mds_ph_lav_product 
              WHERE 
                upper(
                  rtrim(itg_mds_ph_lav_product.active):: TEXT
                ) = 'Y' :: CHARACTER VARYING :: TEXT
            ) b ON ltrim(
              rtrim(a.sku_code):: TEXT, 
              '0' :: CHARACTER VARYING :: TEXT
            ) = ltrim(
              rtrim(b.item_cd):: TEXT, 
              '0' :: CHARACTER VARYING :: TEXT
            ) 
            LEFT JOIN (
              SELECT 
                DISTINCT ltrim(
                  itg_mds_ph_pos_customers.brnch_cd :: TEXT, 
                  '0' :: CHARACTER VARYING :: TEXT
                ) AS brnch_cd, 
                upper(
                  itg_mds_ph_pos_customers.cust_cd :: TEXT
                ) AS cust_cd, 
                itg_mds_ph_pos_customers.brnch_nm, 
                itg_mds_ph_pos_customers.store_mtrx 
              FROM 
                itg_mds_ph_pos_customers 
              WHERE 
                upper(
                  rtrim(
                    itg_mds_ph_pos_customers.active
                  ):: TEXT
                ) = 'Y' :: CHARACTER VARYING :: TEXT
            ) c ON upper(
              rtrim(c.brnch_cd)
            ) = upper(
              rtrim(a.branch_code):: TEXT
            ) 
            AND upper(
              rtrim(c.cust_cd)
            ) = upper(
              rtrim(a.ret_nm_prefix :: TEXT)
            )
        ) b ON upper(
          rtrim(b.store_mtrx):: TEXT
        ) = upper(
          rtrim(a.channeldesc):: TEXT
        ) 
        AND rtrim(a.franchisecode):: TEXT = rtrim(b.scard_franchise_cd):: TEXT 
        AND rtrim(a.brandid):: TEXT = rtrim(b.scard_brand_cd):: TEXT 
        AND rtrim(a.putupid):: TEXT = rtrim(b.scard_put_up_cd):: TEXT 
        AND b.osa_check_date >= a.startdate 
        AND b.osa_check_date <= a.enddate
    ) ippmo 
    LEFT JOIN (
      SELECT 
        DISTINCT itg_mds_ph_lav_product.item_cd, 
        itg_mds_ph_lav_product.scard_franchise_desc, 
        itg_mds_ph_lav_product.scard_brand_desc, 
        itg_mds_ph_lav_product.scard_varient_desc, 
        itg_mds_ph_lav_product.scard_put_up_desc 
      FROM 
        itg_mds_ph_lav_product 
      WHERE 
        upper(
          rtrim(itg_mds_ph_lav_product.active):: TEXT
        ) = 'Y' :: CHARACTER VARYING :: TEXT
    ) implp ON ltrim(
      rtrim(implp.item_cd):: TEXT, 
      '0' :: CHARACTER VARYING :: TEXT
    ) = ltrim(
      rtrim(ippmo.sku_code):: TEXT, 
      '0' :: CHARACTER VARYING :: TEXT
    ) 
    LEFT JOIN (
      SELECT 
        DISTINCT itg_ph_ps_retailer_soldto_map.retailer_name, 
        itg_ph_ps_retailer_soldto_map.sold_to 
      FROM 
        itg_ph_ps_retailer_soldto_map
    ) ipprsm ON upper(
      trim(ipprsm.retailer_name :: TEXT)
    ) = upper(
      trim(ippmo.retailer_name :: TEXT)
    ) 
    LEFT JOIN (
      SELECT 
        impip.soldtocode, 
        impip.parentcustomername, 
        imprpc.rpt_grp_12_desc, 
        imprpc.rpt_grp_1_desc 
      FROM 
        (
          SELECT 
            DISTINCT itg_mds_ph_ise_parent.soldtocode, 
            itg_mds_ph_ise_parent.parentcustomercode, 
            itg_mds_ph_ise_parent.parentcustomername 
          FROM 
            itg_mds_ph_ise_parent
        ) impip 
        LEFT JOIN (
          SELECT 
            DISTINCT itg_mds_ph_ref_parent_customer.parent_cust_cd, 
            itg_mds_ph_ref_parent_customer.rpt_grp_12_desc, 
            itg_mds_ph_ref_parent_customer.rpt_grp_1_desc 
          FROM 
            itg_mds_ph_ref_parent_customer 
          WHERE 
            upper(
              rtrim(
                itg_mds_ph_ref_parent_customer.active
              ):: TEXT
            ) = 'Y' :: CHARACTER VARYING :: TEXT
        ) imprpc ON rtrim(impip.parentcustomercode):: TEXT = rtrim(imprpc.parent_cust_cd):: TEXT
    ) cust ON rtrim(ipprsm.sold_to):: TEXT = rtrim(cust.soldtocode):: TEXT 
    LEFT JOIN (
      SELECT 
        DISTINCT edw_vw_os_time_dim."year", 
        edw_vw_os_time_dim.mnth_id, 
        edw_vw_os_time_dim.cal_date 
      FROM 
        edw_vw_os_time_dim
    ) evotd ON ippmo.osa_check_date = evotd.cal_date 
  WHERE 
    (
      ippmo.osa_flag :: TEXT = '1' :: CHARACTER VARYING :: TEXT 
      OR ippmo.osa_flag :: TEXT = '0' :: CHARACTER VARYING :: TEXT
    ) 
    AND date_part(
      year, 
      ippmo.osa_check_date :: TIMESTAMP without TIME zone
    ) >= (
      date_part(
        year, 
        current_timestamp()
      ) -2 :: DOUBLE PRECISION
    )
),
union_8 as (
SELECT dataset,
        customerid,
        salespersonid,
        to_date(mrch_resp_startdt) AS mrch_resp_startdt,
        to_date(mrch_resp_enddt) AS mrch_resp_enddt,
        survey_enddate,
        question_text AS questiontext,
        cast(value AS VARCHAR) AS value,
        mustcarryitem,
        answerscore,
        presence,
        outofstock,
        kpi,
        scheduleddate,
        vst_status,
        fisc_yr,
        fisc_per,
        firstname,
        lastname,
        customername,
        country,
        storereference,
        store_type AS storetype,
        channel,
        salesgroup,
        prod_hier_l3,
        prod_hier_l4,
        prod_hier_l5,
        prod_hier_l6,
        category,
        segment,
        kpi_chnl_wt,
        mkt_share,
        ques_desc,
        "y/n_flag"
FROM vw_ph_clobotics_perfect_store
WHERE date_part(year, scheduleddate) >= (date_part(year, scheduleddate) - 2)
),
final as (
select * from union_1
union all
select * from union_2
union all
select * from union_3
union all
select * from union_4
union all
select * from union_5
union all
select * from union_6
union all
select * from union_7
union all
select * from union_8
)
select * from final