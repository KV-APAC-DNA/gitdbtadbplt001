with wks_indonesia_inventory_health_analysis_propagation as(
    SELECT * FROM {{ ref('idnwks_integration__wks_indonesia_inventory_health_analysis_propagation') }}
), 
wkly_avg as(
	SELECT *
	FROM {{ ref('idnwks_integration__wks_indonesia_sellout_for_inv_analysis') }}
), 
healthy_inv as(
	SELECT *
	FROM {{ ref('idnwks_integration__wks_indonesia_inventory_healthy_unhealthy_analysis') }}
),

transformed as(
	SELECT inv.YEAR,
        inv.YEAR_QUARTER,
        inv.MONTH_YEAR,
        inv.MONTH_NUMBER,
        inv.COUNTRY_NAME,
        inv.DSTRBTR_GRP_CD,
        inv.DSTRBTR_GRP_NAME,
        inv.FRANCHISE,
        inv.BRAND,
        inv.PROD_SUB_BRAND,
        inv.VARIANT,
        inv.SEGMENT,
        inv.PROD_SUBSEGMENT,
        inv.PROD_CATEGORY,
        inv.PROD_SUBCATEGORY,
        inv.PUT_UP_DESCRIPTION,
        inv.SKU_CD,
        inv.SKU_DESCRIPTION,
        inv.PKA_PRODUCT_KEY,
        inv.PKA_PRODUCT_KEY_DESCRIPTION,
        inv.PRODUCT_KEY,
        inv.PRODUCT_KEY_DESCRIPTION,
        inv.FROM_CCY,
        inv.TO_CCY,
        inv.EXCH_RATE,
        inv.SAP_PRNT_CUST_KEY,
        inv.SAP_PRNT_CUST_DESC,
        inv.SAP_CUST_CHNL_KEY,
        inv.SAP_CUST_CHNL_DESC,
        inv.SAP_CUST_SUB_CHNL_KEY,
        inv.SAP_SUB_CHNL_DESC,
        inv.SAP_GO_TO_MDL_KEY,
        inv.SAP_GO_TO_MDL_DESC,
        inv.SAP_BNR_KEY,
        inv.SAP_BNR_DESC,
        inv.SAP_BNR_FRMT_KEY,
        inv.SAP_BNR_FRMT_DESC,
        inv.RETAIL_ENV,
        inv.REGION,
        inv.ZONE_OR_AREA,
        inv.SI_SLS_QTY,
        inv.SI_GTS_VAL,
        inv.SI_GTS_VAL_USD,
        inv.INVENTORY_QUANTITY,
        inv.INVENTORY_VAL,
        inv.INVENTORY_VAL_USD,
        inv.SO_SLS_QTY,
        inv.SO_GRS_TRD_SLS,
        inv.SO_GRS_TRD_SLS_USD,
        inv.SI_ALL_DB_VAL,
        inv.SI_ALL_DB_VAL_USD,
        inv.SI_INV_DB_VAL,
        inv.SI_INV_DB_VAL_USD,
        inv.LAST_3MONTHS_SO_QTY,
        inv.LAST_6MONTHS_SO_QTY,
        inv.LAST_12MONTHS_SO_QTY,
        inv.LAST_3MONTHS_SO_VAL,
        inv.LAST_3MONTHS_SO_VAL_USD,
        inv.LAST_6MONTHS_SO_VAL,
        inv.LAST_6MONTHS_SO_VAL_USD,
        inv.LAST_12MONTHS_SO_VAL,
        inv.LAST_12MONTHS_SO_VAL_USD,
        inv.PROPAGATE_FLAG,
        inv.PROPAGATE_FROM,
        inv.REASON,
        inv.LAST_36MONTHS_SO_VAL,
		healthy_inv.healthy_inventory as healthy_inventory,
		wkly_avg.min_date,
		datediff(week, wkly_avg.min_date, last_day(to_date(left(month_year, 4) || right(month_year, 2), 'yyyymm'))) diff_weeks,
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
	FROM wks_indonesia_inventory_health_analysis_propagation inv
	LEFT JOIN wkly_avg ON inv.dstrbtr_grp_cd = wkly_avg.dstrbtr_grp_cd
		AND inv.dstrbtr_grp_name = wkly_avg.dstrbtr_grp_name
		AND inv.sap_prnt_cust_key = wkly_avg.SAP_PRNT_CUST_KEY
		AND inv.put_up_description = wkly_avg.pka_size_desc
		AND inv.brand = wkly_avg.GPH_PROD_BRND
		AND inv.variant = wkly_avg.GPH_PROD_VRNT
		AND inv.segment = wkly_avg.GPH_PROD_SGMNT
		AND inv.prod_category = wkly_avg.GPH_PROD_CTGRY
		AND inv.pka_product_key = wkly_avg.pka_product_key
	LEFT JOIN healthy_inv ON month_year = healthy_inv.month
		AND inv.dstrbtr_grp_cd = healthy_inv.dstrbtr_grp_cd
		AND inv.dstrbtr_grp_name = healthy_inv.dstrbtr_grp_name
		AND inv.sap_prnt_cust_key = healthy_inv.SAP_PRNT_CUST_KEY
		AND inv.put_up_description = healthy_inv.pka_size_desc
		AND inv.brand = healthy_inv.GPH_PROD_BRND
		AND inv.variant = healthy_inv.GPH_PROD_VRNT
		AND inv.segment = healthy_inv.GPH_PROD_SGMNT
		AND inv.prod_category = healthy_inv.GPH_PROD_CTGRY
		AND inv.pka_product_key = healthy_inv.pka_product_key
)
select * from transformed