{{
    config(
        sql_header='use warehouse DEV_DNA_CORE_app2_wh;'
    )
}}
with 
itg_in_rtlsalesman as 
(select * from inditg_integration.itg_in_rtlsalesman),
edw_retailer_calendar_dim as 
(
select * from indedw_integration.edw_retailer_calendar_dim
),
edw_retailer_dim as 
(
select * from indedw_integration.edw_retailer_dim
),
v_retail_fran_chanl as 
(
select * from indedw_integration.v_retail_fran_chanl
),
itg_distributoractivation as 
(
select * from inditg_integration.itg_distributoractivation
),
itg_retailerroute as 
(
select * from inditg_integration.itg_retailerroute
),
itg_salesmanmaster as 
(
select * from inditg_integration.itg_salesmanmaster
),
edw_product_dim as 
(
select * from indedw_integration.edw_product_dim
),
v_retailer_udc_map as 
(
select * from indedw_integration.v_retailer_udc_map
),
edw_customer_dim as 
(
select * from indedw_integration.edw_customer_dim
),
edw_dailysales_fact as 
(
select * from indedw_integration.edw_dailysales_fact
),
itg_in_rcustomerroute as 
(
select * from inditg_integration.itg_in_rcustomerroute
),
itg_in_rroute as 
(
select * from inditg_integration.itg_in_rroute
),
itg_in_rsalesmanroute as 
(
select * from inditg_integration.itg_in_rsalesmanroute
),
itg_in_rsalesman as 
(
select * from inditg_integration.itg_in_rsalesman
),
itg_in_rtlheader as 
(
select * from inditg_integration.itg_in_rtlheader
),
itg_in_rrsrheader as 
(
select * from inditg_integration.itg_in_rrsrheader
),
itg_in_rrsrdistributor as 
(
select * from inditg_integration.itg_in_rrsrdistributor
),
itg_in_rretailergeoextension as 
(
select * from inditg_integration.itg_in_rretailergeoextension
),
final as 
(
SELECT sf.customer_code
	,sf.invoice_no
	,sf.invoice_date
	,sf.retailer_code
	,sf.product_code
	,sf.quantity
	,sf.nr_value
	,sf.achievement_nr_val AS achievement_nr
	,sf.saleflag AS invoice_status
	,sf.salesman_code
	,sf.route_code
	,CASE 
		WHEN (((da.activestatus)::CHARACTER VARYING)::TEXT = ('1'::CHARACTER VARYING)::TEXT)
			THEN 'Active'::CHARACTER VARYING
		WHEN (((da.activestatus)::CHARACTER VARYING)::TEXT = ('0'::CHARACTER VARYING)::TEXT)
			THEN 'Inactive'::CHARACTER VARYING
		ELSE 'NA'::CHARACTER VARYING
		END AS active_flag
	,rd.rtrlatitude
	,rd.rtrlongitude
	,rd.rtruniquecode
	,ism.uniquesalescode
	,rd.createddate
	,CASE 
		WHEN (cud.customer_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.customer_name
		END AS customer_name
	,CASE 
		WHEN (cud.region_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.region_name
		END AS region_name
	,CASE 
		WHEN (cud.zone_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.zone_name
		END AS zone_name
	,CASE 
		WHEN (cud.territory_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.territory_name
		END AS territory_name
	,CASE 
		WHEN (cud.territory_classification IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.territory_classification
		END AS territory_classification
	,CASE 
		WHEN (rd.class_desc IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.class_desc
		END AS class_desc
	,CASE 
		WHEN (rd.outlet_type IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.outlet_type
		END AS outlet
	,CASE 
		WHEN (rd.channel_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.channel_name
		END AS channel_name
	,CASE 
		WHEN (rd.business_channel IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.business_channel
		END AS business_channel
	,CASE 
		WHEN (rd.loyalty_desc IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.loyalty_desc
		END AS loyalty_desc
	,CASE 
		WHEN (rd.status_desc IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.status_desc
		END AS status_desc
	,CASE 
		WHEN (pd.product_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.product_name
		END AS product_name
	,CASE 
		WHEN (pd.product_desc IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.product_desc
		END AS product_desc
	,CASE 
		WHEN (pd.franchise_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.franchise_name
		END AS franchise_name
	,CASE 
		WHEN (pd.brand_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.brand_name
		END AS brand_name
	,CASE 
		WHEN (pd.product_category_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.product_category_name
		END AS product_category_name
	,CASE 
		WHEN (pd.variant_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.variant_name
		END AS variant_name
	,CASE 
		WHEN (pd.mothersku_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE pd.mothersku_name
		END AS mothersku_name
	,cd.day
	,cd.mth_mm
	,cd.mth_yyyymm
	,cd.qtr
	,cd.yyyyqtr
	,cd.cal_yr
	,cd.fisc_yr
	,cd.week
	,cd.month_nm_shrt AS "month"
	,CASE 
		WHEN (sf.retailer_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE sf.retailer_name
		END AS retailer_name
	,CASE 
		WHEN (sf.salesman_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE sf.salesman_name
		END AS salesman_name
	,CASE 
		WHEN (sf.route_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE sf.route_name
		END AS route_name
	,CASE 
		WHEN (rd.retailer_address1 IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.retailer_address1
		END AS retailer_address1
	,CASE 
		WHEN (rd.retailer_address2 IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.retailer_address2
		END AS retailer_address2
	,CASE 
		WHEN (rd.retailer_address3 IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.retailer_address3
		END AS retailer_address3
	,CASE 
		WHEN (rd.zone_classification IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.zone_classification
		END AS zone_classification
	,CASE 
		WHEN (rd.state_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE rd.state_name
		END AS state_name
	,CASE 
		WHEN (cud.town_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.town_name
		END AS town_name
	,CASE 
		WHEN (cud.town_classification IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.town_classification
		END AS town_classification
	,sf.achievement_amt
	,sf.gross_amt
	,sf.net_amt
	,sf.no_of_lines AS num_lines
	,sf.prd_sch_disc_amt AS prd_disc_sch_amt
	,sf.tax_amt
	,sf.qps_amt
	,sf.qps_qty
	,sf.sku_rec_amt
	,sf.sku_rec_qty
	,sf.no_of_reco_sku_lines AS num_recsku_lines
	,rd.rtruniquecode AS num_buying_retailers
	,(((rd.rtruniquecode)::TEXT || ('_'::CHARACTER VARYING)::TEXT) || (sf.invoice_no)::TEXT) AS num_bills
	,sf.product_code AS num_packs
	,udc.udc_hsacounter
	,udc.udc_keyaccountname
	,udc.udc_pharmacychain
	,udc.udc_onedetailingbaby
	,udc.udc_sanprovisibilitysss
	,udc.udc_sssconsoffer
	,udc.udc_platinumclub2018
	,udc.udc_sssendcaps
	,udc.udc_platinumclub2019
	,udc.udc_signature2019
	,udc.udc_premium2019
	,udc.udc_gstn
	,udc.udc_platinumq22019new
	,udc.udc_sssprogram2019
	,udc.udc_umangq32019
	,udc.udc_sssscheme2019
	,udc.udc_ssspromoter2019
	,udc.udc_rtrtypeattr
	,udc.udc_bhagidariq32019
	,udc.udc_directorclubq32019
	,udc.udc_platinumq32019new
	,udc.udc_platinumq22019
	,udc.udc_platinumq32019
	,udc.udc_bbastore
	,udc.udc_avbabybodydocq42019
	,udc.udc_orslcac2019
	,udc.udc_babyprofesionalcac2019
	,udc.udc_platinumq42019
	,udc.udc_schemesss2020
	,udc.udc_platinumq12020
	,udc.udc_platinumq32020
	,udc.udc_sssq32020
	,udc.udc_bhagidariq32020
	,udc.udc_directorclubq32020
	,udc.udc_umangq32020
	,udc.udc_daudq32020
	,udc.udc_daudq42020
	,udc.udc_platinumq42020
	,udc.udc_directorclubq42020
	,udc.udc_sssprogramq42020
	,udc.udc_bhagidariq42020
	,udc.udc_umangq42020
	,udc.udc_samriddhi
	,udc.udc_sssprogramq12021
	,udc.udc_platinumq12021
	,udc.udc_orslcac2021
	,udc.udc_bhagidariq12021
	,udc.udc_daudq12021
	,udc.udc_directorclubq12021
	,udc.udc_umangq12021
	,udc.udc_platinumq22021
	,udc.udc_sssprogramq22021
	,udc.udc_bhagidariq22021
	,udc.udc_daudq22021
	,udc.udc_directorclubq22021
	,udc.udc_umangq22021
	,udc.udc_platinumq32021
	,udc.udc_sssprogramq32021
	,udc.udc_newgtm
	,udc.udc_sssprogramq12022
	,udc.udc_ssspharmacystore
	,udc.udc_ssstotstores
	,udc.udc_platinumq12022
	,udc.udc_hsacounterq12022
	,udc.udc_bssaveenoudc2022
	,udc.udc_sssprogramq22022
	,udc.udc_platinumq22022
	,udc.udc_hsacounterq22022
	,udc.udc_ecommerce
	,udc.udc_babytopstoreactivation
	,udc.udc_platinumq32022
	,udc.udc_hsacounterq32022
	,udc.udc_platinumq42022
	,udc.udc_sssprogramq42022
	,udc.udc_sssprogramq12023
	,udc.udc_hsacounterq12023
	,udc.udc_platinumq12023
	,udc.udc_aarogyam
	,udc.udc_sssprogramq22023
	,udc.udc_ssshyperstores2023
	,udc.udc_winbirth2023
	,udc.udc_winclinic2023
	,udc.udc_hsacounterq22023
	,udc.udc_hsacounterq32023
	,udc.udc_aveenosssstores
	,udc.udc_hsacounterq42023
	,udc.udc_hsacounterq12024
	,udc.udc_q124bssprogram
	,udc.udc_specialtyprofessional2024
	,rd.retailer_category_cd
	,rd.retailer_category_name
	,rd.csrtrcode
	,NULL AS abi_ntid
	,NULL AS flm_ntid
	,NULL AS bdm_ntid
	,NULL AS rsm_ntid
	,udc.crt_dttm
	,udc.updt_dttm
	,CASE 
		WHEN (cud.type_name IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.type_name
		END AS type_name
	,CASE 
		WHEN (cud.customer_type IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE cud.customer_type
		END AS customer_type
	,pd.mothersku_code
	,CASE 
		WHEN (sf.STATUS IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		WHEN ((sf.STATUS)::TEXT = (''::CHARACTER VARYING)::TEXT)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE sf.STATUS
		END AS salesman_status
	,(edw.total_retailers)::INTEGER AS total_retailers
	,sf.prdfreeqty AS prd_free_qty
	,CASE 
		WHEN (fc.retailer_channel_level1 IS NULL)
			THEN 'Others'::CHARACTER VARYING
		WHEN ((fc.retailer_channel_level1)::TEXT = (''::CHARACTER VARYING)::TEXT)
			THEN 'Others'::CHARACTER VARYING
		ELSE fc.retailer_channel_level1
		END AS retailer_channel_1
	,CASE 
		WHEN (fc.retailer_channel_level2 IS NULL)
			THEN 'Unknown'::CHARACTER VARYING
		WHEN ((fc.retailer_channel_level2)::TEXT = (''::CHARACTER VARYING)::TEXT)
			THEN 'Unknown'::CHARACTER VARYING
		ELSE fc.retailer_channel_level2
		END AS retailer_channel_2
	,CASE 
		WHEN (fc.retailer_channel_level3 IS NULL)
			THEN 'Others'::CHARACTER VARYING
		WHEN ((fc.retailer_channel_level3)::TEXT = (''::CHARACTER VARYING)::TEXT)
			THEN 'Others'::CHARACTER VARYING
		ELSE fc.retailer_channel_level3
		END AS retailer_channel_3
	,CASE 
		WHEN (fc.report_channel IS NULL)
			THEN 'Non Chemist'::CHARACTER VARYING
		WHEN ((fc.report_channel)::TEXT = (''::CHARACTER VARYING)::TEXT)
			THEN 'Non Chemist'::CHARACTER VARYING
		ELSE fc.report_channel
		END AS report_channel
	,COALESCE(ret_cust_1_1.customer_code, ret_cust_1_n.customer_code, sf.customer_code) AS latest_customer_code
	,COALESCE(ret_cust_1_1.customer_name, ret_cust_1_n.customer_name, cud.customer_name, 'Unknown'::CHARACTER VARYING) AS latest_customer_name
	,cd_terr.territory_code AS latest_territory_code
	,COALESCE(cd_terr.territory_name, cud.territory_name, 'Unknown'::CHARACTER VARYING) AS latest_territory_name
	,rtl.tlcode
	,rtl.tlname
	,rsr.rsrcode
	,rsr.rsrname
	,rgeo.townname AS nielsen_townname
	,rgeo.statename AS nielsen_statename
	,rgeo.districtname AS nielsen_districtname
	,rgeo.subdistrictname AS nielsen_subdistrictname
	,RGEO.type AS nielsen_type
	,rgeo.villagename AS nielsen_villagename
	,rgeo.pincode AS nielsen_pincode
	,rgeo.uacheck AS nielsen_uacheck
	,rgeo.uaname AS nielsen_uaname
	,rgeo.popstrata AS nielsen_popstrata
	,lat_sm.smcode AS latest_salesman_code
	,lat_sm.smname AS latest_salesman_name
	,lat_sm.uniquesalescode AS latest_uniquesalescode
FROM (
	(
		(
			(
				(
					(
						(
							(
								(
									(
										(
											(
												(
													(
														(
															(
																edw_dailysales_fact sf LEFT JOIN edw_retailer_calendar_dim cd ON ((sf.invoice_date = cd.day))
																) LEFT JOIN (
																SELECT derived_table1.rn
																	,derived_table1.retailer_code
																	,derived_table1.rtrlatitude
																	,derived_table1.rtrlongitude
																	,derived_table1.rtruniquecode
																	,derived_table1.createddate
																	,derived_table1.start_date
																	,derived_table1.end_date
																	,derived_table1.customer_code
																	,derived_table1.customer_name
																	,derived_table1.retailer_name
																	,derived_table1.retailer_address1
																	,derived_table1.retailer_address2
																	,derived_table1.retailer_address3
																	,derived_table1.region_code
																	,derived_table1.region_name
																	,derived_table1.zone_code
																	,derived_table1.zone_name
																	,derived_table1.zone_classification
																	,derived_table1.territory_code
																	,derived_table1.territory_name
																	,derived_table1.territory_classification
																	,derived_table1.state_code
																	,derived_table1.state_name
																	,derived_table1.town_code
																	,derived_table1.town_name
																	,derived_table1.town_classification
																	,derived_table1.class_code
																	,derived_table1.class_desc
																	,derived_table1.outlet_type
																	,derived_table1.channel_code
																	,derived_table1.channel_name
																	,derived_table1.business_channel
																	,derived_table1.loyalty_desc
																	,derived_table1.registration_date
																	,derived_table1.status_cd
																	,derived_table1.status_desc
																	,derived_table1.csrtrcode
																	,derived_table1.crt_dttm
																	,derived_table1.updt_dttm
																	,derived_table1.actv_flg
																	,derived_table1.retailer_category_cd
																	,derived_table1.retailer_category_name
																FROM (
																	SELECT row_number() OVER (
																			PARTITION BY edw_retailer_dim.customer_code
																			,edw_retailer_dim.retailer_code ORDER BY edw_retailer_dim.end_date DESC
																			) AS rn
																		,edw_retailer_dim.retailer_code
																		,edw_retailer_dim.rtrlatitude
																		,edw_retailer_dim.rtrlongitude
																		,edw_retailer_dim.rtruniquecode
																		,edw_retailer_dim.createddate
																		,edw_retailer_dim.start_date
																		,edw_retailer_dim.end_date
																		,edw_retailer_dim.customer_code
																		,edw_retailer_dim.customer_name
																		,edw_retailer_dim.retailer_name
																		,edw_retailer_dim.retailer_address1
																		,edw_retailer_dim.retailer_address2
																		,edw_retailer_dim.retailer_address3
																		,edw_retailer_dim.region_code
																		,edw_retailer_dim.region_name
																		,edw_retailer_dim.zone_code
																		,edw_retailer_dim.zone_name
																		,edw_retailer_dim.zone_classification
																		,edw_retailer_dim.territory_code
																		,edw_retailer_dim.territory_name
																		,edw_retailer_dim.territory_classification
																		,edw_retailer_dim.state_code
																		,edw_retailer_dim.state_name
																		,edw_retailer_dim.town_code
																		,edw_retailer_dim.town_name
																		,edw_retailer_dim.town_classification
																		,edw_retailer_dim.class_code
																		,edw_retailer_dim.class_desc
																		,edw_retailer_dim.outlet_type
																		,edw_retailer_dim.channel_code
																		,edw_retailer_dim.channel_name
																		,edw_retailer_dim.business_channel
																		,edw_retailer_dim.loyalty_desc
																		,edw_retailer_dim.registration_date
																		,edw_retailer_dim.status_cd
																		,edw_retailer_dim.status_desc
																		,edw_retailer_dim.csrtrcode
																		,edw_retailer_dim.crt_dttm
																		,edw_retailer_dim.updt_dttm
																		,edw_retailer_dim.actv_flg
																		,edw_retailer_dim.retailer_category_cd
																		,edw_retailer_dim.retailer_category_name
																		,edw_retailer_dim.file_rec_dt
																	FROM edw_retailer_dim
																	) derived_table1
																WHERE (derived_table1.rn = 1)
																) rd ON (
																	(
																		((sf.customer_code)::TEXT = (rd.customer_code)::TEXT)
																		AND ((sf.retailer_code)::TEXT = (rd.retailer_code)::TEXT)
																		)
																	)
															) LEFT JOIN v_retail_fran_chanl fc ON (
																(
																	((rd.customer_code)::TEXT = (fc.customer_code)::TEXT)
																	AND ((rd.retailer_code)::TEXT = (fc.retailer_code)::TEXT)
																	)
																)
														) LEFT JOIN (
														SELECT rd.customer_code
															,rd.status_desc
															,rr.rmcode
															,sm.smcode
															,count(DISTINCT rd.retailer_code) AS total_retailers
														FROM (
															(
																(
																	edw_retailer_dim rd LEFT JOIN itg_distributoractivation da ON (((rd.customer_code)::TEXT = (da.distcode)::TEXT))
																	) LEFT JOIN (
																	SELECT itg_retailerroute.distcode
																		,itg_retailerroute.rtrcode
																		,itg_retailerroute.rmcode
																		,itg_retailerroute.rmname
																	FROM itg_retailerroute
																	WHERE (
																			(
																				itg_retailerroute.file_rec_dt = (
																					SELECT "max" (itg_retailerroute.file_rec_dt) AS "max"
																					FROM itg_retailerroute
																					)
																				)
																			AND ((itg_retailerroute.routetype)::TEXT = ('Sales Route'::CHARACTER VARYING)::TEXT)
																			)
																	) rr ON (
																		(
																			((rd.customer_code)::TEXT = (rr.distcode)::TEXT)
																			AND ((rd.retailer_code)::TEXT = (rr.rtrcode)::TEXT)
																			)
																		)
																) LEFT JOIN (
																SELECT itg_salesmanmaster.distcode
																	,itg_salesmanmaster.rmcode
																	,itg_salesmanmaster.smcode
																	,itg_salesmanmaster.smname
																	,itg_salesmanmaster.STATUS
																FROM itg_salesmanmaster
																WHERE (
																		itg_salesmanmaster.file_rec_dt = (
																			SELECT "max" (itg_salesmanmaster.file_rec_dt) AS "max"
																			FROM itg_salesmanmaster
																			)
																		)
																) sm ON (
																	(
																		((rd.customer_code)::TEXT = (sm.distcode)::TEXT)
																		AND ((rr.rmcode)::TEXT = (sm.rmcode)::TEXT)
																		)
																	)
															)
														WHERE (
																(
																	rd.file_rec_dt = (
																		SELECT "max" (edw_retailer_dim.file_rec_dt) AS "max"
																		FROM edw_retailer_dim
																		)
																	)
																AND ((rd.status_desc)::TEXT = ('Active'::CHARACTER VARYING)::TEXT)
																)
														GROUP BY rd.customer_code
															,rd.status_desc
															,rr.rmcode
															,sm.smcode
														) edw ON (
															(
																(
																	((sf.customer_code)::TEXT = (edw.customer_code)::TEXT)
																	AND ((sf.salesman_code)::TEXT = (edw.smcode)::TEXT)
																	)
																AND ((sf.route_code)::TEXT = (edw.rmcode)::TEXT)
																)
															)
													) LEFT JOIN edw_product_dim pd ON (((sf.product_code)::TEXT = (pd.product_code)::TEXT))
												) LEFT JOIN edw_customer_dim cud ON (((sf.customer_code)::TEXT = (cud.customer_code)::TEXT))
											) LEFT JOIN itg_distributoractivation da ON (((sf.customer_code)::TEXT = (da.distcode)::TEXT))
										) LEFT JOIN v_retailer_udc_map udc ON (
											(
												((sf.customer_code)::TEXT = (udc.customer_code_udc)::TEXT)
												AND ((sf.retailer_code)::TEXT = (udc.retailer_code_udc)::TEXT)
												)
											)
									) LEFT JOIN (
									SELECT derived_table1.distcode
										,derived_table1.smcode
										,derived_table1.uniquesalescode
									FROM (
										SELECT DISTINCT itg_salesmanmaster.distcode
											,itg_salesmanmaster.smcode
											,itg_salesmanmaster.uniquesalescode
											,row_number() OVER (
												PARTITION BY itg_salesmanmaster.distcode
												,itg_salesmanmaster.smcode ORDER BY (itg_salesmanmaster.createddate) DESC NULLS LAST
													,itg_salesmanmaster.uniquesalescode DESC NULLS LAST
												) AS rn
										FROM itg_salesmanmaster
										) derived_table1
									WHERE (derived_table1.rn = 1)
									) ism ON (
										(
											((sf.customer_code)::TEXT = (ism.distcode)::TEXT)
											AND ((sf.salesman_code)::TEXT = (ism.smcode)::TEXT)
											)
										)
								) LEFT JOIN (
								SELECT ret.rtruniquecode
									,ret.customer_code
									,ret.customer_name
								FROM (
									edw_retailer_dim ret JOIN edw_customer_dim cd ON (
											(
												((ret.customer_code)::TEXT = (cd.customer_code)::TEXT)
												AND ((cd.active_flag)::TEXT = ('Y'::CHARACTER VARYING)::TEXT)
												)
											)
									)
								WHERE (
										(
											(ret.actv_flg = 'Y'::char)
											AND ((ret.status_desc)::TEXT = ('Active'::CHARACTER VARYING)::TEXT)
											)
										AND (
											ret.rtruniquecode IN (
												SELECT DISTINCT ret1.rtruniquecode
												FROM (
													edw_retailer_dim ret1 JOIN edw_customer_dim cd ON (
															(
																((ret1.customer_code)::TEXT = (cd.customer_code)::TEXT)
																AND ((cd.active_flag)::TEXT = ('Y'::CHARACTER VARYING)::TEXT)
																)
															)
													)
												WHERE (
														(ret1.actv_flg = 'Y'::char)
														AND ((ret1.status_desc)::TEXT = ('Active'::CHARACTER VARYING)::TEXT)
														)
												GROUP BY ret1.rtruniquecode
												HAVING (count(DISTINCT ret1.customer_code) = 1)
												)
											)
										)
								GROUP BY ret.rtruniquecode
									,ret.customer_code
									,ret.customer_name
								) ret_cust_1_1 ON (((rd.rtruniquecode)::TEXT = (ret_cust_1_1.rtruniquecode)::TEXT))
							) LEFT JOIN (
							SELECT rd1.rtruniquecode
								,rd1.customer_code
								,rd1.customer_name
								,row_number() OVER (
									PARTITION BY rd1.rtruniquecode ORDER BY rd1.customer_code DESC
									) AS cust_rnk
							FROM (
								edw_dailysales_fact sales JOIN (
									SELECT edw_retailer_dim.rtruniquecode
										,edw_retailer_dim.retailer_code
										,edw_retailer_dim.customer_code
										,edw_retailer_dim.customer_name
										,row_number() OVER (
											PARTITION BY edw_retailer_dim.customer_code
											,edw_retailer_dim.retailer_code ORDER BY edw_retailer_dim.end_date DESC
											) AS rn
									FROM edw_retailer_dim
									) rd1 ON (
										(
											(
												((sales.customer_code)::TEXT = (rd1.customer_code)::TEXT)
												AND ((sales.retailer_code)::TEXT = (rd1.retailer_code)::TEXT)
												)
											AND (rd1.rn = 1)
											)
										)
								)
							WHERE (
									sales.invoice_date = (
										SELECT "max" (sales1.invoice_date) AS "max"
										FROM (
											edw_dailysales_fact sales1 JOIN (
												SELECT edw_retailer_dim.rtruniquecode
													,edw_retailer_dim.retailer_code
													,edw_retailer_dim.customer_code
													,row_number() OVER (
														PARTITION BY edw_retailer_dim.customer_code
														,edw_retailer_dim.retailer_code ORDER BY edw_retailer_dim.end_date DESC
														) AS rn
												FROM edw_retailer_dim
												) rd2 ON (
													(
														(
															((sales1.customer_code)::TEXT = (rd2.customer_code)::TEXT)
															AND ((sales1.retailer_code)::TEXT = (rd2.retailer_code)::TEXT)
															)
														AND (rd2.rn = 1)
														)
													)
											)
										WHERE ((rd2.rtruniquecode)::TEXT = (rd1.rtruniquecode)::TEXT)
										)
									)
							GROUP BY rd1.rtruniquecode
								,rd1.customer_code
								,rd1.customer_name
							) ret_cust_1_n ON (
								(
									((rd.rtruniquecode)::TEXT = (ret_cust_1_n.rtruniquecode)::TEXT)
									AND (ret_cust_1_n.cust_rnk = 1)
									)
								)
						) LEFT JOIN (
						SELECT edw_customer_dim.customer_code
							,edw_customer_dim.territory_code
							,edw_customer_dim.territory_name
						FROM edw_customer_dim
						GROUP BY edw_customer_dim.customer_code
							,edw_customer_dim.territory_code
							,edw_customer_dim.territory_name
						) cd_terr ON (((COALESCE(ret_cust_1_1.customer_code, ret_cust_1_n.customer_code, sf.customer_code))::TEXT = (cd_terr.customer_code)::TEXT))
					) LEFT JOIN (
					SELECT cr.distcode
						,"substring" (
							(cr.rtrcode)::TEXT
							,7
							) AS rtrcode
						,cr.rmcode
						,r.rmname
						,s.smcode
						,s.smname
						,s.STATUS
						,s.uniquesalescode
						,row_number() OVER (
							PARTITION BY cr.distcode
							,"substring" (
								(cr.rtrcode)::TEXT
								,7
								) ORDER BY s.smcode DESC
							) AS sm_rnk
					FROM itg_in_rcustomerroute cr
						,itg_in_rroute r
						,itg_in_rsalesmanroute rs
						,itg_in_rsalesman s
					WHERE (
							(
								(
									(
										(
											(
												(
													((cr.routetype)::CHARACTER(10) = 'S'::char)
													AND ((cr.distcode)::TEXT = (r.distcode)::TEXT)
													)
												AND ((cr.rmcode)::TEXT = (r.rmcode)::TEXT)
												)
											AND ((rs.distrcode)::TEXT = (r.distcode)::TEXT)
											)
										AND ((rs.routecode)::TEXT = (r.rmcode)::TEXT)
										)
									AND ((s.distcode)::TEXT = (rs.distrcode)::TEXT)
									)
								AND ((s.smcode)::TEXT = (rs.salesmancode)::TEXT)
								)
							AND ((s.STATUS)::CHARACTER(10) = 'Y'::char)
							)
					) lat_sm ON (
						(
							(
								((COALESCE(ret_cust_1_1.customer_code, ret_cust_1_n.customer_code, sf.customer_code))::TEXT = (lat_sm.distcode)::TEXT)
								AND ((sf.retailer_code)::TEXT = ((lat_sm.rtrcode)::CHARACTER VARYING)::TEXT)
								)
							AND (lat_sm.sm_rnk = 1)
							)
						)
				) LEFT JOIN (
				SELECT tlsm.distrcode
					,tlsm.salesmancode
					,tlsm.tlcode
					,tlhd.tlname
					,row_number() OVER (
						PARTITION BY tlsm.distrcode
						,tlsm.salesmancode ORDER BY tlsm.tlcode DESC
						) AS rn
				FROM (
					itg_in_rtlsalesman tlsm JOIN itg_in_rtlheader tlhd ON (
							(
								((tlsm.tlcode)::TEXT = (tlhd.tlcode)::TEXT)
								AND (tlsm.isactive = 'Y'::char)
								)
							)
					)
				) rtl ON (
					(
						(
							((COALESCE(ret_cust_1_1.customer_code, ret_cust_1_n.customer_code, sf.customer_code))::TEXT = (rtl.distrcode)::TEXT)
							AND ((COALESCE(lat_sm.smcode, sf.salesman_code))::TEXT = (rtl.salesmancode)::TEXT)
							)
						AND (rtl.rn = 1)
						)
					)
			) LEFT JOIN (
			SELECT rsrd.distrcode
				,rsrd.rsrcode
				,rsrh.rsrname
				,row_number() OVER (
					PARTITION BY rsrd.distrcode ORDER BY rsrd.rsrcode DESC
					) AS rn
			FROM (
				itg_in_rrsrdistributor rsrd JOIN itg_in_rrsrheader rsrh ON (
						(
							((rsrd.rsrcode)::TEXT = (rsrh.rsrcode)::TEXT)
							AND (rsrd.isactive = 'Y'::char)
							)
						)
				)
			) rsr ON (
				(
					((COALESCE(ret_cust_1_1.customer_code, ret_cust_1_n.customer_code, sf.customer_code))::TEXT = (rsr.distrcode)::TEXT)
					AND (rsr.rn = 1)
					)
				)
		) LEFT JOIN (
		SELECT itg_in_rretailergeoextension.cmpcutomercode
			,itg_in_rretailergeoextension.townname
			,itg_in_rretailergeoextension.statename
			,itg_in_rretailergeoextension.districtname
			,itg_in_rretailergeoextension.subdistrictname
			,ITG_IN_RRETAILERGEOEXTENSION.type
			,itg_in_rretailergeoextension.villagename
			,itg_in_rretailergeoextension.pincode
			,itg_in_rretailergeoextension.uacheck
			,itg_in_rretailergeoextension.uaname
			,itg_in_rretailergeoextension.popstrata
			,row_number() OVER (
				PARTITION BY itg_in_rretailergeoextension.cmpcutomercode ORDER BY itg_in_rretailergeoextension.distrcode DESC
				) AS urc_rnk
		FROM itg_in_rretailergeoextension
		) rgeo ON (
			(
				((rd.rtruniquecode)::TEXT = (rgeo.cmpcutomercode)::TEXT)
				AND (rgeo.urc_rnk = 1)
				)
			)
	)
WHERE (cd.fisc_yr >= 2014))
select * from final