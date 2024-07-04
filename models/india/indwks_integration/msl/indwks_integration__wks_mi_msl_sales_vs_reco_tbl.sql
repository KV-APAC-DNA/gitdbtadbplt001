with v_rpt_sales_details as(
    select * from {{ ref('indedw_integration__v_rpt_sales_details') }}.
),
wks_mi_msl_lkp_mnthly_tbl as(
    select * from {{ ref('indwks_integration__wks_mi_msl_lkp_mnthly_tbl') }}
),
edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}.
),
WKS_MI_MSL_RET_DIM_SUBD_CNT as(
    select * from {{ ref('indwks_integration__wks_mi_msl_ret_dim_subd_cnt') }}
),
sales_cube_mi_msl AS (
    SELECT mth_mm,
        customer_code,
        customer_name,
        retailer_code,
        retailer_name,
        rtruniquecode,
        region_name,
        zone_name,
        territory_name,
        channel_name,
        retailer_category_name,
        salesman_code,
        salesman_name,
        unique_sales_code,
        latest_customer_code,
        latest_customer_name,
        latest_territory_code,
        latest_territory_name,
        latest_salesman_code,
        latest_salesman_name,
        latest_uniquesalescode,
        mothersku_code AS mothersku_code_sold,
        mothersku_name AS mothersku_name_sold,
        SUM(quantity) AS quantity,
        SUM(achievement_nr) AS achievement_nr
    FROM v_rpt_sales_details sd
    WHERE mth_mm >= 202201
        AND UPPER(channel_name) = 'SUB-STOCKIEST'
    GROUP BY mth_mm,
        customer_code,
        customer_name,
        retailer_code,
        retailer_name,
        rtruniquecode,
        region_name,
        zone_name,
        territory_name,
        channel_name,
        retailer_category_name,
        salesman_code,
        salesman_name,
        unique_sales_code,
        latest_customer_code,
        latest_customer_name,
        latest_territory_code,
        latest_territory_name,
        latest_salesman_code,
        latest_salesman_name,
        latest_uniquesalescode,
        mothersku_code,
        mothersku_name
),
transformed as(
    SELECT sc.*,
		msl.qtr,
		msl.period,
		msl.msl_count,
		pd.mothersku_code AS mothersku_code_recom,
		sku.mothersku_name AS mothersku_name_recom,
		CASE 
			WHEN sku.mothersku_name IS NULL
				THEN '0'
			ELSE '1'
			END AS ms_flag,
		--  sm.smcode, sm.smname, sm.uniquesalescode, 
		subd.total_subd FROM sales_cube_mi_msl sc LEFT JOIN wks_mi_msl_lkp_mnthly_tbl sku ON UPPER(TRIM(sc.region_name)) = UPPER(TRIM(sku.region_name))
		AND UPPER(TRIM(sc.zone_name)) = UPPER(TRIM(sku.zone_name))
		AND UPPER(TRIM(sc.mothersku_name_sold)) = UPPER(TRIM(sku.mothersku_name))
		AND sc.mth_mm = sku.mth_mm LEFT JOIN (
		SELECT period,
			qtr,
			mth_mm,
			region_name,
			zone_name,
			COUNT(DISTINCT mothersku_name) AS msl_count
		FROM wks_mi_msl_lkp_mnthly_tbl
		GROUP BY 1,
			2,
			3,
			4,
			5
		) msl ON UPPER(TRIM(sc.region_name)) = UPPER(TRIM(msl.region_name))
		AND UPPER(TRIM(sc.zone_name)) = UPPER(TRIM(msl.zone_name))
		AND sc.mth_mm = msl.mth_mm
		/*LEFT JOIN wks_skurecom_mi_msl_rsalesman sm
       ON sc.customer_code = sm.distcode
      AND sc.retailer_code = sm.rtrcode */
		LEFT JOIN (
		SELECT mothersku_code,
			mothersku_name
		FROM edw_product_dim
		WHERE NVL(delete_flag, 'XYZ') <> 'N'
		GROUP BY 1,
			2
		) pd ON UPPER(TRIM(sku.mothersku_name)) = UPPER(TRIM(pd.mothersku_name)) LEFT JOIN wks_mi_msl_ret_dim_subd_cnt subd ON UPPER(TRIM(sc.region_name)) = UPPER(TRIM(subd.region_name))
		AND UPPER(TRIM(sc.zone_name)) = UPPER(TRIM(subd.zone_name))
		AND UPPER(TRIM(sc.territory_name)) = UPPER(TRIM(subd.territory_name))
		AND UPPER(TRIM(sc.retailer_category_name)) = UPPER(TRIM(subd.retailer_category_name))
)
select * from transformed