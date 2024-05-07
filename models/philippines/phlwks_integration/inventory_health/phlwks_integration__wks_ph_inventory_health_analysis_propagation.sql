with wks_ph_regional as(
    select * from {{ ref('phlwks_integration__wks_ph_regional') }}
),
vw_edw_reg_exch_rate as(
    select * from {{ ref('aspedw_integration__vw_edw_reg_exch_rate') }}
),
edw_copa_trans_fact as(
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
v_edw_customer_sales_dim as(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
wks_ph_inventory_health_analysis_propagation_prestep as(
    select * from {{ ref('phlwks_integration__wks_ph_inventory_health_analysis_propagation_prestep') }}
),
Regional AS (
			SELECT *,
				SUM(SI_GTS_VAL) OVER (
					PARTITION BY country_name,
					year,
					MNTH_ID
					) AS SI_INV_DB_VAL,
				SUM(SI_GTS_VAL_USD) OVER (
					PARTITION BY country_name,
					year,
					MNTH_ID
					) AS SI_INV_DB_VAL_USD
			FROM wks_ph_regional
			WHERE (country_name || SAP_PRNT_CUST_DESC) IN (
					SELECT country_name || SAP_PRNT_CUST_DESC
					FROM (
						SELECT country_name,
							SAP_PRNT_CUST_DESC,
							NVL(SUM(INVENTORY_VAL), 0) AS INV_VAL,
							NVL(SUM(SO_GRS_TRD_SLS), 0) AS Sellout_val
						FROM wks_ph_regional
						WHERE SAP_PRNT_CUST_DESC IS NOT NULL
						GROUP BY country_name,
							SAP_PRNT_CUST_DESC
						HAVING INV_VAL <> 0
						)
					)
			),
		COPA AS (
			WITH RegionalCurrency AS (
					SELECT cntry_key,
						cntry_nm,
						rate_type,
						from_ccy,
						to_ccy,
						valid_date,
						jj_year,
						jj_mnth_id AS MNTH_ID,
						(cast(EXCH_RATE AS NUMERIC(15, 5))) AS EXCH_RATE
					FROM vw_edw_reg_exch_rate
					WHERE cntry_key = 'PH'
						AND jj_mnth_id >= (DATE_PART(YEAR, current_timestamp()) - 2)
						AND to_ccy = 'USD'
					),
				GTS AS (
					SELECT ctry_key,
						obj_crncy_co_obj,
						caln_yr_mo,
						fisc_yr,
						sum(SI_ALL_DB_VAL) AS gts_value,
						sum(CASE 
								WHEN avail_customer IS NULL
									THEN 0
								ELSE si_all_db_val
								END) AS si_inv_db_val
					FROM (
						WITH sellin_all AS (
								SELECT ctry_key,
									obj_crncy_co_obj,
									prnt_cust_key,
									caln_yr_mo,
									fisc_yr,
									(cast(gts AS NUMERIC(38, 15))) AS gts
								FROM (
									SELECT copa.ctry_key AS ctry_key,
										obj_crncy_co_obj,
										cus_sales_extn.prnt_cust_key,
										substring(fisc_yr_per, 1, 4) || substring(fisc_yr_per, 6, 2) AS caln_yr_mo,
										fisc_yr,
										SUM(amt_obj_crncy) AS gts
									FROM edw_copa_trans_fact copa
									LEFT JOIN edw_company_dim cmp ON copa.co_cd = cmp.co_cd
									LEFT JOIN v_edw_customer_sales_dim cus_sales_extn ON copa.sls_org = cus_sales_extn.sls_org
										AND copa.dstr_chnl = cus_sales_extn.dstr_chnl::TEXT
										AND copa.div = cus_sales_extn.div
										AND copa.cust_num = cus_sales_extn.cust_num
									WHERE cmp.ctry_group = 'Philippines'
										AND left(fisc_yr_per, 4) >= (DATE_PART(YEAR, current_timestamp()) - 2)
										AND copa.cust_num IS NOT NULL
										AND copa.acct_hier_shrt_desc = 'GTS'
										AND amt_obj_crncy > 0
									GROUP BY 1,
										2,
										3,
										4,
										5
									)
								),
							available_customers AS (
								SELECT mnth_id,
									country_name,
									sap_prnt_cust_key,
									sap_prnt_cust_desc,
									sum(si_gts_val) AS si_gts_val,
									sum(si_sls_qty) AS si_sls_qty
								FROM wks_ph_inventory_health_analysis_propagation_prestep inv
								WHERE country_name IN ('Philippines')
								GROUP BY 1,
									2,
									3,
									4
								HAVING (
										sum(inventory_quantity) <> 0
										OR sum(inventory_val) <> 0
										)
								ORDER BY 1 DESC,
									2,
									3,
									4
								)
						SELECT a.ctry_key,
							a.obj_crncy_co_obj,
							a.caln_yr_mo,
							a.fisc_yr,
							a.prnt_cust_key AS total_customer,
							b.sap_prnt_cust_key AS avail_customer,
							sum(gts) AS SI_ALL_DB_VAL
						FROM sellin_all a
						LEFT JOIN available_customers b ON b.mnth_id = a.caln_yr_mo
							AND a.prnt_cust_key = b.sap_prnt_cust_key
						GROUP BY 1,
							2,
							3,
							4,
							5,
							6
						ORDER BY 1 DESC,
							2,
							3,
							4
						)
					GROUP BY 1,
						2,
						3,
						4
					)
			SELECT ctry_key,
				obj_crncy_co_obj,
				caln_yr_mo,
				fisc_yr,
				(cast(gts_value AS NUMERIC(38, 5))) AS gts,
				si_inv_db_val,
				CASE 
					WHEN ctry_key = 'PH'
						THEN cast((gts_value * exch_rate) / 1000 AS NUMERIC(38, 5))
					END AS GTS_USD,
				CASE 
					WHEN ctry_key = 'PH'
						THEN cast((si_inv_db_val * exch_rate) / 1000 AS NUMERIC(38, 5))
					END AS si_inv_db_val_usd
			FROM gts,
				RegionalCurrency
			WHERE GTS.obj_crncy_co_obj = RegionalCurrency.from_ccy
				AND RegionalCurrency.MNTH_ID = (
					SELECT max(MNTH_ID)
					FROM RegionalCurrency
					)
			),
transformed as(
	SELECT year,
		qrtr_no,
		mnth_id,
		mnth_no,
		country_name,
		dstrbtr_grp_cd,
		dstrbtr_grp_cd_nm,
		global_prod_franchise,
		global_prod_brand,
		global_prod_sub_brand,
		global_prod_variant,
		global_prod_segment,
		global_prod_subsegment,
		global_prod_category,
		global_prod_subcategory,
		pka_size_desc AS global_put_up_desc,
		sku_cd,
		sku_description,
		--greenlight_sku_flag,
		pka_product_key,
		pka_product_key_description,
		product_key,
		product_key_description,
		from_ccy,
		to_ccy,
		exch_rate,
		sap_prnt_cust_key,
		sap_prnt_cust_desc,
		sap_cust_chnl_key,
		sap_cust_chnl_desc,
		sap_cust_sub_chnl_key,
		sap_sub_chnl_desc,
		sap_go_to_mdl_key,
		sap_go_to_mdl_desc,
		sap_bnr_key,
		sap_bnr_desc,
		sap_bnr_frmt_key,
		sap_bnr_frmt_desc,
		retail_env,
		region,
		zone_or_area,
		round(cast(si_sls_qty AS NUMERIC(38, 5)), 5) AS si_sls_qty,
		round(cast(si_gts_val AS NUMERIC(38, 5)), 5) AS si_gts_val,
		round(cast(si_gts_val_usd AS NUMERIC(38, 5)), 5) AS si_gts_val_usd,
		round(cast(inventory_quantity AS NUMERIC(38, 5)), 5) AS inventory_quantity,
		round(cast(inventory_val AS NUMERIC(38, 5)), 5) AS inventory_val,
		round(cast(inventory_val_usd AS NUMERIC(38, 5)), 5) AS inventory_val_usd,
		round(cast(so_sls_qty AS NUMERIC(38, 5)), 5) AS so_sls_qty,
		round(cast(so_grs_trd_sls AS NUMERIC(38, 5)), 5) AS so_trd_sls,
		so_grs_trd_sls_usd AS so_trd_sls_usd,
		round(cast(COPA.gts AS NUMERIC(38, 5)), 5) AS SI_ALL_DB_VAL,
		round(cast(COPA.gts_usd AS NUMERIC(38, 5)), 5) AS SI_ALL_DB_VAL_USD,
		round(cast(COPA.si_inv_db_val AS NUMERIC(38, 5)), 5) AS si_inv_db_val,
		round(cast(COPA.si_inv_db_val_usd AS NUMERIC(38, 5)), 5) AS si_inv_db_val_usd,
		last_3months_so_qty,
		last_6months_so_qty,
		last_12months_so_qty,
		last_3months_so_val,
		last_3months_so_val_usd,
		last_6months_so_val,
		last_6months_so_val_usd,
		last_12months_so_val,
		last_12months_so_val_usd,
		propagate_flag,
		propagate_from,
		reason,
		last_36months_so_val
	FROM Regional,
		COPA
	WHERE Regional.year = COPA.fisc_yr
		AND Regional.mnth_id = COPA.caln_yr_mo
		AND Regional.from_ccy = COPA.obj_crncy_co_obj
),
final as(
    select * from transformed
)
select * from final