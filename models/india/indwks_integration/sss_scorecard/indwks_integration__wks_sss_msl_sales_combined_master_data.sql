with itg_mds_in_sss_ps_msl as(
    select * from {{ ref('inditg_integration__itg_mds_in_sss_ps_msl') }}
),
edw_retailer_calendar_dim as(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
wks_sss_sales_base_data_for_msl as(
    select * from {{ ref('indwks_integration__wks_sss_sales_base_data_for_msl') }}
),
edw_product_dim as(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
msl as(
    SELECT DISTINCT MSL.re,
			MSL.sku_code,
			MSL.sku_name,
			to_date(MSL.valid_from) AS valid_from,
			to_date(MSL.valid_to) AS valid_to,
			cal_yr,
			qtr
		FROM itg_mds_in_sss_ps_msl MSL --in_sdl.sdl_mds_in_ps_msl MSL
		JOIN edw_retailer_calendar_dim cal ON caldate BETWEEN valid_from
				AND valid_to
),
sales as(
    SELECT DISTINCT region_name,
			zone_name,
			territory_name,
			channel_name,
			retail_environment,
			salesman_code,
			salesman_name,
			distributor_code,
			distributor_name,
			qtr,
			cal_yr,
			store_code,
			store_name,
			rtruniquecode
		FROM wks_sss_sales_base_data_for_msl
),
pd as(
    SELECT DISTINCT franchise_name,
			brand_name,
			product_category_name,
			variant_name,
			mothersku_name
		FROM edw_product_dim
),
transformed as(
    SELECT Sales.*,
        MSL1.prod_hier_l3,
        MSL1.prod_hier_l4,
        MSL1.prod_hier_l5,
        MSL1.prod_hier_l6,
        MSL1.prod_hier_l9
    FROM (
        SELECT MSL.re,
            MSL.sku_code,
            MSL.sku_name,
            cal_yr,
            qtr,
            CASE 
                WHEN UPPER(FRANCHISE_NAME) = 'SANPRO'
                    THEN 'STAYFREE'
                WHEN UPPER(FRANCHISE_NAME) = 'OTC'
                    THEN 'LISTERINE'
                WHEN UPPER(FRANCHISE_NAME) = 'BEAUTY CARE'
                    THEN 'C&C'
                WHEN UPPER(FRANCHISE_NAME) = 'BABY CARE'
                    THEN 'JOHNSON BABY'
                END AS PROD_HIER_L3,
            brand_name PROD_HIER_L4,
            product_category_name PROD_HIER_L5,
            variant_name PROD_HIER_L6,
            mothersku_name PROD_HIER_L9
        FROM MSL
        LEFT JOIN pd ON UPPER(MSL.sku_name) = UPPER(pd.mothersku_name)
        ) MSL1, sales
    WHERE UPPER(MSL1.re) = UPPER(SALES.RETAIL_ENVIRONMENT)
        AND SALES.cal_yr = MSL1.cal_yr
        AND SALES.qtr = MSL1.qtr
        --    AND (MSL1.re, MSL1.sku_name, MSL1.cal_yr, MSL1.qtr)  not in 
        --      ( SELECT distinct retail_environment, product_code , cal_yr,	qtr	 
        --        FROM  wks_sss_sales_msl_matching_data ) 
)
select * from transformed