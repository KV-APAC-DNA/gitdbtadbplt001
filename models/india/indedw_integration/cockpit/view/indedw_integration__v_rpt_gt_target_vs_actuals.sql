with v_customer_weekly_sales as 
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.V_CUSTOMER_WEEKLY_SALES
),
edw_customer_dim as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.EDW_CUSTOMER_DIM
),
edw_product_dim as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.EDW_PRODUCT_DIM
),
edw_retailer_calendar_dim as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.edw_retailer_calendar_dim
),
edw_retailer_dim as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.edw_retailer_dim
),
edw_salesman_target as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.edw_salesman_target
),
itg_salesmanmaster as 
(
    select * from DEV_DNA_CORE.INDITG_INTEGRATION.itg_salesmanmaster
),
edw_gt_target_dim as
(
    select * from DEV_DNA_CORE.INDEDW_INTEGRATION.edw_gt_target_dim
),
cte1 as
(SELECT rc.fisc_yr AS year,
		rc.month_nm_shrt AS month,
		rc.week,
		gtf.customer_code,
		gtf.retailer_code,
		cud.customer_name,
		gtf.salesman_code,
		sm.smname AS salesman_name,
		pd.brand_name,
		gtf.route_code,
		gtf.route_name,
		cud.region_name,
		cud.region_code,
		cud.zone_code,
		cud.zone_name,
		cud.territory_code,
		cud.territory_name,
		cud.territory_classification,
		cud.state_code,
		cud.state_name,
		cud.town_code,
		cud.town_name,
		cud.town_classification,
		cud.city,
		cud.type_code,
		cud.type_name,
		NULL AS targetgt,
		sum(CASE 
				WHEN (gtf.achievement_nr_val IS NULL)
					THEN ((0)::NUMERIC)::NUMERIC(18, 0)
				ELSE gtf.achievement_nr_val
				END) AS nrvalue,
		NULL AS targetbills,
		(gtf.bills) AS bills,
		NULL AS targetpack,
		sum(CASE 
				WHEN (gtf.packs IS NULL)
					THEN (0)::BIGINT
				ELSE gtf.packs
				END) AS packs,
		smt.sm_tgt_amt,
		smt.measure_type,
		NULL AS abi_ntid,
		NULL AS flm_ntid,
		NULL AS bdm_ntid,
		NULL AS rsm_ntid,
		gtf.customer_code AS num_buying_retailers,
		cud.active_flag,
		CASE 
			WHEN (rd.status_desc IS NULL)
				THEN 'Unknown'
			ELSE rd.status_desc
			END AS status_desc,
		sum(CASE 
				WHEN (gtf.achievement_amt IS NULL)
					THEN ((0)::NUMERIC)::NUMERIC(18, 0)
				ELSE gtf.achievement_amt
				END) AS achievement_amt,
		CASE 
			WHEN (rd.channel_name IS NULL)
				THEN 'Unknown'
			ELSE rd.channel_name
			END AS channel_name,
		gtf.inv_day AS day,
		CASE 
			WHEN (
					(gtf.salesman_status IS NULL)
					OR ((gtf.salesman_status)::TEXT = ('')::TEXT)
					)
				THEN 'Unknown'
			ELSE gtf.salesman_status
			END AS salesman_status
	FROM (
		(
			(
				(
					(
						(
							(
								SELECT 
                                    trim(v_customer_weekly_sales.customer_code) as customer_code,
									trim(v_customer_weekly_sales.retailer_code) as retailer_code,
									trim(v_customer_weekly_sales.salesman_code) as salesman_code,
									trim(v_customer_weekly_sales.salesman_name) as salesman_name,
									v_customer_weekly_sales.product_code,
									v_customer_weekly_sales.route_code,
									v_customer_weekly_sales.route_name,
									v_customer_weekly_sales.inv_year,
									v_customer_weekly_sales.inv_month,
									v_customer_weekly_sales.inv_week,
									v_customer_weekly_sales.achievement_nr_val,
									v_customer_weekly_sales.bills,
									v_customer_weekly_sales.packs,
									v_customer_weekly_sales.retailer_name,
									v_customer_weekly_sales.achievement_amt,
									v_customer_weekly_sales.inv_day,
									v_customer_weekly_sales.salesman_status
								FROM v_customer_weekly_sales
								WHERE ((v_customer_weekly_sales.inv_year)::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2)
								) gtf LEFT JOIN edw_customer_dim cud ON (((gtf.customer_code)::TEXT = (cud.customer_code)::TEXT))
							) LEFT JOIN edw_product_dim pd ON (((gtf.product_code)::TEXT = (pd.product_code)::TEXT))
						) LEFT JOIN (
						SELECT DISTINCT edw_retailer_calendar_dim.fisc_yr,
							edw_retailer_calendar_dim.month_nm_shrt,
							edw_retailer_calendar_dim.mth_yyyymm,
							edw_retailer_calendar_dim.week
						FROM edw_retailer_calendar_dim edw_retailer_calendar_dim
						WHERE ((edw_retailer_calendar_dim.fisc_yr)::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2)
						) rc ON (
							(
								(
									(gtf.inv_year = rc.fisc_yr)
									AND (gtf.inv_month = rc.mth_yyyymm)
									)
								AND (gtf.inv_week = rc.week)
								)
							)
					) LEFT JOIN (
					SELECT derived_table1.rn,
						derived_table1.retailer_code,
						derived_table1.customer_code,
						derived_table1.retailer_name,
						derived_table1.channel_name,
						derived_table1.status_desc,
						derived_table1.retailer_address1,
						derived_table1.retailer_address2,
						derived_table1.retailer_address3,
						derived_table1.zone_classification,
						derived_table1.state_name,
						derived_table1.class_desc,
						derived_table1.outlet_type,
						derived_table1.business_channel,
						derived_table1.loyalty_desc
					FROM (
						SELECT row_number() OVER (
								PARTITION BY edw_retailer_dim.customer_code,
								edw_retailer_dim.retailer_code ORDER BY edw_retailer_dim.end_date DESC
								) AS rn,
							edw_retailer_dim.retailer_code,
							edw_retailer_dim.customer_code,
							edw_retailer_dim.retailer_name,
							edw_retailer_dim.channel_name,
							edw_retailer_dim.status_desc,
							edw_retailer_dim.retailer_address1,
							edw_retailer_dim.retailer_address2,
							edw_retailer_dim.retailer_address3,
							edw_retailer_dim.zone_classification,
							edw_retailer_dim.state_name,
							edw_retailer_dim.class_desc,
							edw_retailer_dim.outlet_type,
							edw_retailer_dim.business_channel,
							edw_retailer_dim.loyalty_desc
						FROM edw_retailer_dim edw_retailer_dim
						) derived_table1
					WHERE (derived_table1.rn = 1)
					) rd ON (
						(
							((gtf.customer_code)::TEXT = (rd.customer_code)::TEXT)
							AND ((gtf.retailer_code)::TEXT = (rd.retailer_code)::TEXT)
							)
						)
				) LEFT JOIN edw_salesman_target smt ON (
					(
						(
							(
								(
									(
										((gtf.customer_code)::TEXT = (smt.dist_code)::TEXT)
										AND ((gtf.salesman_code)::TEXT = (smt.sm_code)::TEXT)
										)
									AND (gtf.inv_year = smt.fisc_year)
									)
								AND ((rc.month_nm_shrt)::TEXT = (smt.month_nm)::TEXT)
								)
							AND ((rd.channel_name)::TEXT = (smt.channel)::TEXT)
							)
						AND ((pd.brand_name)::TEXT = (smt.brand_focus)::TEXT)
						)
					)
			) LEFT JOIN (
			SELECT derived_table1.distcode,
				derived_table1.smcode,
				derived_table1.rmcode,
				derived_table1.createddate,
				derived_table1.STATUS,
				derived_table1.smname
			FROM (
				SELECT 
                    trim(itg_salesmanmaster.distcode) as distcode,
					trim(itg_salesmanmaster.smcode) as smcode,
					trim(itg_salesmanmaster.rmcode) as rmcode,
					itg_salesmanmaster.createddate,
					initcap((itg_salesmanmaster.STATUS)::TEXT) AS STATUS,
					itg_salesmanmaster.smname,
					row_number() OVER (
						PARTITION BY itg_salesmanmaster.distcode,
						itg_salesmanmaster.smcode,
						itg_salesmanmaster.rmcode ORDER BY itg_salesmanmaster.createddate DESC
						) AS rnk
				FROM itg_salesmanmaster
				) derived_table1
			WHERE (derived_table1.rnk = 1)
			) sm ON (
				(
					(
						(trim(gtf.customer_code)::TEXT = trim(sm.distcode)::TEXT)
						AND (trim(gtf.salesman_code)::TEXT = trim(sm.smcode)::TEXT)
						)
					AND (trim(gtf.route_code)::TEXT = trim(sm.rmcode)::TEXT)
					)
				)
		)
	GROUP BY rc.fisc_yr,
		rc.month_nm_shrt,
		rc.week,
		gtf.inv_year,
		gtf.inv_month,
		gtf.inv_week,
		gtf.customer_code,
		gtf.retailer_code,
		cud.customer_name,
		gtf.salesman_code,
		sm.smname,
		pd.brand_name,
		gtf.route_code,
		gtf.route_name,
		cud.region_name,
		cud.region_code,
		cud.zone_code,
		cud.zone_name,
		cud.territory_code,
		cud.territory_name,
		cud.territory_classification,
		cud.state_code,
		cud.state_name,
		cud.town_code,
		cud.town_name,
		cud.town_classification,
		cud.city,
		cud.type_code,
		cud.type_name,
		smt.sm_tgt_amt,
		smt.measure_type,
		cud.active_flag,
		rd.status_desc,
		rd.channel_name,
		gtf.inv_day,
		gtf.salesman_status,
		gtf.bills),
cte2 as
(SELECT rc2.fisc_yr AS year,
		rc2.month_nm_shrt AS month,
		rc2.week,
		gtd.customer_code,
		NULL AS retailer_code,
		cud2.customer_name,
		gtd.salesman_code,
		sm.smname AS salesman_name,
		NULL AS brand_name,
		gtd.route_code,
		gtd.route_name,
		cud2.region_name,
		cud2.region_code,
		cud2.zone_code,
		cud2.zone_name,
		cud2.territory_code,
		cud2.territory_name,
		cud2.territory_classification,
		cud2.state_code,
		cud2.state_name,
		cud2.town_code,
		cud2.town_name,
		cud2.town_classification,
		cud2.city,
		cud2.type_code,
		cud2.type_name,
		sum(CASE 
				WHEN (gtd.target_gt IS NULL)
					THEN ((0)::NUMERIC)::NUMERIC(18, 0)
				ELSE gtd.target_gt
				END) AS targetgt,
		NULL AS nrvalue,
		sum(CASE 
				WHEN (gtd.target_bills IS NULL)
					THEN ((0)::NUMERIC)::NUMERIC(18, 0)
				ELSE gtd.target_bills
				END) AS targetbills,
		NULL AS bills,
		sum(CASE 
				WHEN (gtd.target_packs IS NULL)
					THEN ((0)::NUMERIC)::NUMERIC(18, 0)
				ELSE gtd.target_packs
				END) AS targetpack,
		NULL AS packs,
		NULL AS sm_tgt_amt,
		NULL AS measure_type,
		NULL AS abi_ntid,
		NULL AS flm_ntid,
		NULL AS bdm_ntid,
		NULL AS rsm_ntid,
		gtd.customer_code AS num_buying_retailers,
		cud2.active_flag,
		NULL AS status_desc,
		NULL AS achievement_amt,
		NULL AS channel_name,
		NULL AS day,
		(
			CASE 
				WHEN (
						(sm.STATUS IS NULL)
						OR (sm.STATUS = ('')::TEXT)
						)
					THEN ('Unknown')::TEXT
				ELSE sm.STATUS
				END
			) AS salesman_status
	FROM (
		(
			(
				(
					SELECT 
                        edw_gt_target_dim.customer_code,
						edw_gt_target_dim.customer_name,
						edw_gt_target_dim.salesman_code,
						edw_gt_target_dim.salesman_name,
						edw_gt_target_dim.route_code,
						edw_gt_target_dim.route_name,
						edw_gt_target_dim.year,
						edw_gt_target_dim.month,
						edw_gt_target_dim.week,
						edw_gt_target_dim.target_gt,
						edw_gt_target_dim.target_bills,
						edw_gt_target_dim.target_packs,
						edw_gt_target_dim.crt_dttm,
						edw_gt_target_dim.updt_dttm
					FROM edw_gt_target_dim edw_gt_target_dim
					WHERE (
							(edw_gt_target_dim.week > 0)
							AND ((edw_gt_target_dim.year)::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2)
							)
					) gtd LEFT JOIN edw_customer_dim cud2 ON (((gtd.customer_code)::TEXT = (cud2.customer_code)::TEXT))
				) LEFT JOIN (
				SELECT DISTINCT edw_retailer_calendar_dim.fisc_yr,
					edw_retailer_calendar_dim.month_nm_shrt,
					edw_retailer_calendar_dim.mth_yyyymm,
					edw_retailer_calendar_dim.week
				FROM edw_retailer_calendar_dim edw_retailer_calendar_dim
				WHERE ((edw_retailer_calendar_dim.fisc_yr)::DOUBLE PRECISION >= (date_part(year,current_timestamp()))-2)
				) rc2 ON (
					(
						(
							(rc2.fisc_yr = gtd.year)
							AND (rc2.mth_yyyymm = gtd.month)
							)
						AND (rc2.week = gtd.week)
						)
					)
			) LEFT JOIN (
			SELECT derived_table1.distcode,
				derived_table1.smcode,
				derived_table1.rmcode,
				derived_table1.createddate,
				derived_table1.STATUS,
				derived_table1.smname
			FROM (
				SELECT 
                    itg_salesmanmaster.distcode,
					itg_salesmanmaster.smcode,
					itg_salesmanmaster.rmcode,
					itg_salesmanmaster.createddate,
					initcap((itg_salesmanmaster.STATUS)::TEXT) AS STATUS,
					itg_salesmanmaster.smname,
					row_number() OVER (
						PARTITION BY itg_salesmanmaster.distcode,
						itg_salesmanmaster.smcode,
						itg_salesmanmaster.rmcode ORDER BY itg_salesmanmaster.createddate DESC
						) AS rnk
				FROM itg_salesmanmaster
				) derived_table1
			WHERE (derived_table1.rnk = 1)
			) sm ON (
				(
					(
						(trim(gtd.customer_code)::TEXT = trim(sm.distcode)::TEXT)
						AND (trim(gtd.salesman_code)::TEXT = trim(sm.smcode)::TEXT)
						)
					AND (trim(gtd.route_code)::TEXT = trim(sm.rmcode)::TEXT)
					)
				)
		)
	GROUP BY rc2.fisc_yr,
		rc2.month_nm_shrt,
		rc2.week,
		gtd.year,
		gtd.month,
		gtd.week,
		gtd.customer_code,
		cud2.customer_name,
		gtd.salesman_code,
		sm.smname,
		gtd.route_code,
		gtd.route_name,
		cud2.region_name,
		cud2.region_code,
		cud2.zone_code,
		cud2.zone_name,
		cud2.territory_code,
		cud2.territory_name,
		cud2.territory_classification,
		cud2.state_code,
		cud2.state_name,
		cud2.town_code,
		cud2.town_name,
		cud2.town_classification,
		cud2.city,
		cud2.type_code,
		cud2.type_name,
		cud2.active_flag,
		sm.STATUS),
transformed as
(
	select * from cte1
	union all
	select * from cte2
)
select * from transformed
