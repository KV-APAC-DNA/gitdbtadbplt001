with wks_ph_inventory_healthy_unhealthy_analysis as(
	SELECT * FROM {{ ref('phlwks_integration__wks_ph_inventory_healthy_unhealthy_analysis') }}
),
wks_ph_sellout_for_inv_analysis as(
	SELECT * FROM {{ ref('phlwks_integration__wks_ph_sellout_for_inv_analysis') }}
),
wks_ph_inventory_health_analysis_propagation as(
	SELECT * FROM {{ ref('phlwks_integration__wks_ph_inventory_health_analysis_propagation') }}
),
transformed as(
SELECT inv.*,
	healthy_inv.healthy_inventory,
	wkly_avg.min_date,
	datediff(week, wkly_avg.min_date, last_day(to_date(left(mnth_id, 4) || right(mnth_id, 2), 'yyyymm'))) diff_weeks,
	CASE 
		WHEN least(diff_weeks, 52) <= 0
			THEN 1
		ELSE least(diff_weeks, 52)
		END AS l12m_weeks,
	CASE 
		WHEN least(diff_weeks, 26) <= 0
			THEN 1
		ELSE least(diff_weeks, 26)
		END AS l6m_weeks,
	CASE 
		WHEN least(diff_weeks, 13) <= 0
			THEN 1
		ELSE least(diff_weeks, 13)
		END AS l3m_weeks,
	inv.last_12months_so_val / l12m_weeks AS l12m_weeks_avg_sales,
	inv.last_6months_so_val / l6m_weeks AS l6m_weeks_avg_sales,
	inv.last_3months_so_val / l3m_weeks AS l3m_weeks_avg_sales,
	last_12months_so_val_usd / l12m_weeks AS l12m_weeks_avg_sales_usd,
	last_6months_so_val_usd / l6m_weeks AS l6m_weeks_avg_sales_usd,
	last_3months_so_val_usd / l3m_weeks AS l3m_weeks_avg_sales_usd,
	last_12months_so_qty / l12m_weeks AS l12m_weeks_avg_sales_qty,
	last_6months_so_qty / l6m_weeks AS l6m_weeks_avg_sales_qty,
	last_3months_so_qty / l3m_weeks AS l3m_weeks_avg_sales_qty
FROM wks_ph_inventory_health_analysis_propagation inv
LEFT JOIN (
	SELECT *
	FROM wks_ph_sellout_for_inv_analysis
	) wkly_avg ON inv.dstrbtr_grp_cd = wkly_avg.dstrbtr_grp_cd
	AND inv.dstrbtr_grp_cd_nm = wkly_avg.DSTRBTR_GRP_CD_nm
	AND inv.sap_prnt_cust_key = wkly_avg.SAP_PRNT_CUST_KEY
	AND inv.global_put_up_desc = wkly_avg.pka_size_desc
	AND inv.global_prod_brand = wkly_avg.GLOBAL_PROD_BRAND
	AND inv.global_prod_variant = wkly_avg.GLOBAL_PROD_VARIANT
	AND inv.global_prod_segment = wkly_avg.GLOBAL_PROD_SEGMENT
	AND inv.global_prod_category = wkly_avg.GLOBAL_PROD_CATEGORY
	AND inv.pka_product_key = wkly_avg.pka_product_key
LEFT JOIN (
     SELECT * FROM wks_ph_inventory_healthy_unhealthy_analysis
	) healthy_inv ON mnth_id = healthy_inv.month
	AND inv.dstrbtr_grp_cd = healthy_inv.dstrbtr_grp_cd
	AND inv.dstrbtr_grp_cd_nm = healthy_inv.DSTRBTR_GRP_CD_nm
	AND inv.sap_prnt_cust_key = healthy_inv.SAP_PRNT_CUST_KEY
	AND inv.global_put_up_desc = healthy_inv.pka_size_desc
	AND inv.global_prod_brand = healthy_inv.GLOBAL_PROD_BRAND
	AND inv.global_prod_variant = healthy_inv.GLOBAL_PROD_VARIANT
	AND inv.global_prod_segment = healthy_inv.GLOBAL_PROD_SEGMENT
	AND inv.global_prod_category = healthy_inv.GLOBAL_PROD_CATEGORY
	AND inv.pka_product_key = healthy_inv.pka_product_key
)
select * from transformed