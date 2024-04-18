with edw_time_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_time_dim
),
edw_pharm_sellout_fact as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_pharm_sellout_fact
),
edw_ps_msl_items as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_ps_msl_items
),
vw_material_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.vw_material_dim
),
edw_perenso_prod_probeid_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_perenso_prod_probeid_dim
),
edw_perenso_account_probeid_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_perenso_account_probeid_dim
),
edw_perenso_prod_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_perenso_prod_dim
),
edw_perenso_account_dim as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_perenso_account_dim
),
edw_perenso_order_fact as(
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.edw_perenso_order_fact
),
itg_perenso_distributor_detail as(
    select * from DEV_DNA_CORE.SNAPPCFITG_INTEGRATION.itg_perenso_distributor_detail
),
union1 as(
SELECT 'IQVIA' AS pac_source_type
	,'PHARMACY' AS pac_subsource_type
	,etd.cal_date::date AS cal_date
	,etd.jj_wk
	,etd.jj_mnth
	,etd.jj_mnth_shrt
	,etd.jj_mnth_long
	,etd.jj_qrtr
	,etd.jj_year
	,etd.cal_mnth_id
	,etd.jj_mnth_id
	,etd.cal_mnth
	,etd.cal_qrtr
	,etd.cal_year
	,etd.jj_mnth_tot
	,etd.jj_mnth_day
	,etd.cal_mnth_nm
	,psf.store_probe_id
	,epsr.acct_id AS store_id
	,epsr.acct_display_name AS store
	,epsr.acct_suburb AS suburb
	,epsr.acct_postcode AS postcode
	,epsr.acct_country AS store_country
	,epsr.acct_region AS store_region
	,epsr.acct_state AS store_state
	,epsr.acct_banner_division AS store_channel
	,epsr.acct_banner_type AS store_type
	,epsr.acct_banner AS store_banner
	,epsr.acct_grade AS store_grade
	,epsr.acct_ssr_team_leader AS ssr_team_leader
	,epsr.acct_ssr_territory AS ssr_territory
	,epsr.acct_ssr_state AS ssr_region
	,epsr.acct_au_pharma_territory AS aus_pharm_territory
	,epsr.acct_au_pharma_state AS aus_pharm_region
	,'NOT ASSIGNED' AS aus_pharm_strategy
	,'NOT ASSIGNED' AS aus_pharm_responsibility
	,(COALESCE((('Territory: '::CHARACTER VARYING)::TEXT || (epsr.acct_type_desc)::TEXT), ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS primary_rep_team
	,(COALESCE(((('Territory: '::CHARACTER VARYING)::TEXT || (epsr.acct_type_desc)::TEXT) || (epsr.acct_au_pharma_state)::TEXT), ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS primary_rep_region
	,(COALESCE((('Territory: '::CHARACTER VARYING)::TEXT || (epsr.acct_au_pharma_territory)::TEXT), ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS primary_rep
	,psf.product_probe_id
	,eppd.prod_key AS product_id
	,eppd.prod_desc AS product
	,NULL AS units_in_inner
	,eppd.prod_id AS sap_code
	,eppd.prod_ean AS ean
	,NULL AS symbion_code
	,NULL AS api_code
	,NULL AS sigma_code
	,eppd.prod_jj_franchise AS pharmacy_franchise
	,eppd.prod_jj_category AS pharmacy_category
	,eppd.prod_jj_brand AS pharmacy_brand
	,eppd.prod_sap_franchise AS sap_franchise
	,eppd.prod_sap_profit_centre AS sap_profit_centre
	,eppd.prod_sap_product_major AS sap_profit_major
	,eppd.prod_pbs AS pbs
	,eppd.prod_active_au_pharma AS active_aus_pharm
	,(ltrim((vmd.matl_id)::TEXT, ((0)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS matl_id
	,vmd.matl_desc
	,vmd.mega_brnd_desc
	,vmd.brnd_desc
	,vmd.fran_desc
	,vmd.grp_fran_desc
	,vmd.prod_fran_desc
	,vmd.prod_mjr_desc
	,vmd.putup_desc
	,vmd.bar_cd
	,psf.time_period
	,psf.units
	,psf.amount
	,psf.derived_price
	,NULL AS order_id
	,NULL AS order_status
	,'0' AS turnin_units
	,NULL AS inners
	,NULL AS turnin_nis
	,'0' AS discount_per
	,'0' AS "discount$"
	,NULL AS line_item_status
	,NULL AS order_date
	,NULL AS sent_date
	,NULL AS distributor_country
	,NULL AS distributor
	,NULL AS distributor_dc
	,NULL AS distributor_account_number
	,NULL AS order_type
	,msl.msl_flag AS pharm_msl_flag
	,(msl.msl_rank)::CHARACTER VARYING(5) AS pharm_msl_rank
FROM (
	(
		(
			(
				(
					edw_pharm_sellout_fact psf LEFT JOIN edw_time_dim etd ON (
							(
								(
									(
										(
											(
												(
													"substring" (
														(psf.time_period)::TEXT
														,5
														,4
														) || "substring" (
														(psf.time_period)::TEXT
														,3
														,2
														)
													) || "substring" (
													(psf.time_period)::TEXT
													,1
													,2
													)
												)
											)::INTEGER
										)::NUMERIC
									)::NUMERIC(18, 0) = etd.time_id
								)
							)
					) LEFT JOIN edw_perenso_account_probeid_dim epsr ON (((psf.store_probe_id)::TEXT = (epsr.account_probe_id)::TEXT))
				) LEFT JOIN edw_perenso_prod_probeid_dim eppd ON (((psf.product_probe_id)::TEXT = (eppd.product_probe_id)::TEXT))
			) LEFT JOIN vw_material_dim vmd ON ((ltrim((eppd.prod_id)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((vmd.matl_id)::TEXT, ((0)::CHARACTER VARYING)::TEXT)))
		) LEFT JOIN edw_ps_msl_items msl ON (
			(
				(
					(ltrim((eppd.prod_ean)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((msl.ean)::TEXT, ((0)::CHARACTER VARYING)::TEXT))
					AND (upper((msl.retail_environment)::TEXT) = ('AU INDY PHARMACY'::CHARACTER VARYING)::TEXT)
					)
				AND (upper((msl.latest_record)::TEXT) = ('Y'::CHARACTER VARYING)::TEXT)
				)
			)
	)
),
union2 as(
SELECT 'PERENSO' AS pac_source_type
	,'ORDER_DETAIL' AS pac_subsource_type
	,etd.cal_date::date AS cal_date
	,etd.jj_wk
	,etd.jj_mnth
	,etd.jj_mnth_shrt
	,etd.jj_mnth_long
	,etd.jj_qrtr
	,etd.jj_year
	,etd.cal_mnth_id
	,etd.jj_mnth_id
	,etd.cal_mnth
	,etd.cal_qrtr
	,etd.cal_year
	,etd.jj_mnth_tot
	,etd.jj_mnth_day
	,etd.cal_mnth_nm
	,NULL AS store_probe_id
	,epod.acct_key AS store_id
	,epsd.acct_display_name AS store
	,epsd.acct_suburb AS suburb
	,epsd.acct_postcode AS postcode
	,epsd.acct_country AS store_country
	,epsd.acct_region AS store_region
	,epsd.acct_state AS store_state
	,epsd.acct_banner_division AS store_channel
	,epsd.acct_banner_type AS store_type
	,epsd.acct_banner AS store_banner
	,epsd.acct_grade AS store_grade
	,epsd.acct_ssr_team_leader AS ssr_team_leader
	,epsd.acct_ssr_territory AS ssr_territory
	,epsd.acct_ssr_state AS ssr_region
	,epsd.acct_au_pharma_territory AS aus_pharm_territory
	,epsd.acct_au_pharma_state AS aus_pharm_region
	,'NOT ASSIGNED' AS aus_pharm_strategy
	,'NOT ASSIGNED' AS aus_pharm_responsibility
	,(COALESCE((('Territory: '::CHARACTER VARYING)::TEXT || (epsd.acct_type_desc)::TEXT), ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS primary_rep_team
	,(COALESCE(((('Territory: '::CHARACTER VARYING)::TEXT || (epsd.acct_type_desc)::TEXT) || (epsd.acct_au_pharma_state)::TEXT), ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS primary_rep_region
	,(COALESCE((('Territory: '::CHARACTER VARYING)::TEXT || (epsd.acct_au_pharma_territory)::TEXT), ('NOT ASSIGNED'::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS primary_rep
	,NULL AS product_probe_id
	,epod.prod_key AS product_id
	,eppd.prod_desc AS product
	,NULL AS units_in_inner
	,eppd.prod_id AS sap_code
	,eppd.prod_ean AS ean
	,NULL AS symbion_code
	,NULL AS api_code
	,NULL AS sigma_code
	,eppd.prod_jj_franchise AS pharmacy_franchise
	,eppd.prod_jj_category AS pharmacy_category
	,eppd.prod_jj_brand AS pharmacy_brand
	,eppd.prod_sap_franchise AS sap_franchise
	,eppd.prod_sap_profit_centre AS sap_profit_centre
	,eppd.prod_sap_product_major AS sap_profit_major
	,eppd.prod_pbs AS pbs
	,eppd.prod_active_au_pharma AS active_aus_pharm
	,(ltrim((vmd.matl_id)::TEXT, ((0)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS matl_id
	,vmd.matl_desc
	,vmd.mega_brnd_desc
	,vmd.brnd_desc
	,vmd.fran_desc
	,vmd.grp_fran_desc
	,vmd.prod_fran_desc
	,vmd.prod_mjr_desc
	,vmd.putup_desc
	,vmd.bar_cd
	,NULL AS time_period
	,'0' AS units
	,'0' AS amount
	,'0' AS derived_price
	,epod.order_key AS order_id
	,epod.order_header_status AS order_status
	,epod.unit_qty AS turnin_units
	,NULL AS inners
	,(epod.entered_qty * epod.nis) AS turnin_nis
	,'0' AS discount_per
	,'0' AS "discount$"
	,epod.order_batch_status AS line_item_status
	,epod.order_date
	,epod.sent_dt AS sent_date
	,NULL AS distributor_country
	,ipdd.distributor
	,ipdd.display_name AS distributor_dc
	,NULL AS distributor_account_number
	,epod.order_type_desc AS order_type
	,msl.msl_flag AS pharm_msl_flag
	,(msl.msl_rank)::CHARACTER VARYING(5) AS pharm_msl_rank
FROM (
	(
		(
			(
				(
					(
						(
							SELECT edw_perenso_order_fact.order_key
								,edw_perenso_order_fact.order_type_key
								,edw_perenso_order_fact.order_type_desc
								,edw_perenso_order_fact.acct_key
								,edw_perenso_order_fact.order_date
								,edw_perenso_order_fact.order_header_status_key
								,edw_perenso_order_fact.charge
								,edw_perenso_order_fact.confirmation
								,edw_perenso_order_fact.diary_item_key
								,edw_perenso_order_fact.work_item_key
								,edw_perenso_order_fact.account_order_no
								,edw_perenso_order_fact.delvry_instns
								,edw_perenso_order_fact.batch_key
								,edw_perenso_order_fact.line_key
								,edw_perenso_order_fact.prod_key
								,edw_perenso_order_fact.unit_qty
								,edw_perenso_order_fact.entered_qty
								,edw_perenso_order_fact.entered_unit_key
								,edw_perenso_order_fact.list_price
								,edw_perenso_order_fact.nis
								,edw_perenso_order_fact.rrp
								,edw_perenso_order_fact.credit_line_key
								,edw_perenso_order_fact.credited
								,edw_perenso_order_fact.disc_key
								,edw_perenso_order_fact.branch_key
								,edw_perenso_order_fact.dist_acct
								,edw_perenso_order_fact.delvry_dt
								,edw_perenso_order_fact.order_batch_status_key
								,edw_perenso_order_fact.suffix
								,edw_perenso_order_fact.sent_dt
								,edw_perenso_order_fact.deal_key
								,edw_perenso_order_fact.deal_desc
								,edw_perenso_order_fact.start_date
								,edw_perenso_order_fact.end_date
								,edw_perenso_order_fact.short_desc
								,edw_perenso_order_fact.discount_desc
								,edw_perenso_order_fact.order_header_status
								,edw_perenso_order_fact.order_batch_status
								,edw_perenso_order_fact.order_currency_cd
							FROM edw_perenso_order_fact
							WHERE ((edw_perenso_order_fact.order_type_desc)::TEXT = ('Turn-In Orders'::CHARACTER VARYING)::TEXT)
							) epod LEFT JOIN edw_time_dim etd ON (((epod.delvry_dt::date) = etd.cal_date::date))
						) LEFT JOIN edw_perenso_account_dim epsd ON ((epod.acct_key = epsd.acct_id))
					) LEFT JOIN edw_perenso_prod_dim eppd ON ((epod.prod_key = eppd.prod_key))
				) LEFT JOIN vw_material_dim vmd ON ((ltrim((eppd.prod_id)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((vmd.matl_id)::TEXT, ((0)::CHARACTER VARYING)::TEXT)))
			) LEFT JOIN edw_ps_msl_items msl ON (
				(
					(
						(ltrim((eppd.prod_ean)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ltrim((msl.ean)::TEXT, ((0)::CHARACTER VARYING)::TEXT))
						AND (upper((msl.retail_environment)::TEXT) = ('AU INDY PHARMACY'::CHARACTER VARYING)::TEXT)
						)
					AND (upper((msl.latest_record)::TEXT) = ('Y'::CHARACTER VARYING)::TEXT)
					)
				)
		) LEFT JOIN itg_perenso_distributor_detail ipdd ON ((((ipdd.branch_key)::CHARACTER VARYING)::TEXT = ((epod.branch_key)::CHARACTER VARYING)::TEXT))
	)
WHERE (
		(upper((epsd.acct_type)::TEXT) = ('PHARMACY'::CHARACTER VARYING)::TEXT)
		AND (upper((epsd.acct_country)::TEXT) = ('AUSTRALIA'::CHARACTER VARYING)::TEXT)
		)
),
final as(
    select * from union1
    UNION ALL
    select * from union2
)
select * from final