with itg_ph_ecommerce_offtake_acommerce as(
    select * from {{ ref('phlitg_integration__itg_ph_ecommerce_offtake_acommerce') }}
),
sdl_mds_ph_ecom_product as(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_ecom_product') }}
),
edw_product_key_attributes as(
    select * from {{ ref('aspedw_integration__edw_product_key_attributes') }}
),
edw_crncy_exch_rates as(
    select * from {{ ref('aspedw_integration__edw_crncy_exch_rates') }}
),
edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
trans as(
SELECT trans.*,
		map.sku1_code AS "mapping_matl_num"
	FROM itg_ph_ecommerce_offtake_acommerce trans
	LEFT JOIN sdl_mds_ph_ecom_product map ON trans.acommerce_item_sku = map.rpc
		AND map.name = 'J&J'
),
a as(
SELECT ctry_nm,
			gcph_franchise,
			pka_franchise_description,
			gcph_category,
			gcph_subcategory,
			pka_brand_description,
			pka_subbranddesc,
			pka_variantdesc,
			pka_subvariantdesc,
			pka_package,
			pka_rootcode,
			pka_productdesc,
			gcph_segment,
			gcph_subsegment,
			LTRIM(matl_num, '0') AS "matl_num",
			pka_sizedesc,
			pka_skuiddesc,
			LTRIM(ean_upc, '0') AS "ean_upc",
			lst_nts AS "nts_date"
		FROM edw_product_key_attributes
		WHERE matl_type_cd IN ('FERT', 'HALB', 'SAPR')
			AND lst_nts IS NOT NULL
			AND ctry_nm = 'Philippines'
		GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
),
pka as(
		-- Get latest product hierarchy for J&J SKUs
	SELECT CASE 
			WHEN a.ctry_nm = 'APSC Regional'
				THEN 'China Skincare'
			ELSE a.ctry_nm
			END AS "ctry_nm",
		a."ean_upc",
		gcph_franchise,
		pka_franchise_description,
		gcph_category,
		gcph_subcategory,
		pka_brand_description,
		pka_subbranddesc,
		pka_variantdesc,
		pka_subvariantdesc,
		pka_package,
		a.pka_rootcode,
		pka_productdesc,
		gcph_segment,
		gcph_subsegment,
		a."matl_num",
		pka_sizedesc,
		pka_skuiddesc
	FROM 
		-- Get GCPH by EAN by latest date
		 a
	JOIN (
		SELECT ctry_nm,
			LTRIM(ean_upc, '0') AS "ean_upc",
			LTRIM(matl_num, '0') AS "matl_num",
			pka_rootcode,
			lst_nts AS "latest_nts_date",
			ROW_NUMBER() OVER (
				PARTITION BY ctry_nm,
				ean_upc,
				matl_num ORDER BY lst_nts DESC
				) AS "row_number"
		FROM edw_product_key_attributes
		WHERE matl_type_cd IN ('FERT', 'HALB', 'SAPR')
			AND lst_nts IS NOT NULL
		GROUP BY ctry_nm,
			ean_upc,
			matl_num,
			pka_rootcode,
			lst_nts
		) b ON a.ctry_nm = b.ctry_nm
		AND a."matl_num" = b."matl_num"
		AND a.pka_rootcode = b.pka_rootcode
		AND "latest_nts_date" = "nts_date"
		AND "row_number" = 1
	GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
),
exch as(
		SELECT *
	FROM (
		SELECT from_crncy,
			to_crncy,
			ex_rt,
			valid_from,
			ROW_NUMBER() OVER (
				PARTITION BY from_crncy,
				to_crncy ORDER BY valid_from DESC
				) AS rank
		FROM edw_crncy_exch_rates
		WHERE from_crncy = 'PHP'
			AND to_crncy = 'USD'
			AND ex_rt_typ = 'BWAR'
			AND valid_from <= current_timestamp()
		GROUP BY 1, 2, 3, 4
		)
	WHERE rank = 1
),
transformed as(

SELECT current_timestamp()::timestamp_ntz(9) AS "load_date",
	filename,
	'Philippines' AS "market",
	shipping_province AS "state",
	cal_yr as fisc_yr,
	cast(cal_yr || '0' || LPAD(cal_mo_2, 2, '0') AS INTEGER) AS fisc_per,
	cal_yr,
	cal_mo_2,
	cal_day,
	marketplace_order_date AS "transaction_date",
	order_date AS "order_date",
	order_id AS "order_id",
	NULL AS "sap_customer_code",
	NULL AS "customer_code",
	NULL AS "distributor_code",
	'ACommerce' AS "distributor_name",
	sub_sales_channel AS "platform_name",
	NULL AS "retailer_name",
	NULL AS "sub_store_name",
	gcph_franchise,
	pka_franchise_description AS "gcph_needstate",
	gcph_category,
	gcph_subcategory,
	pka_brand_description AS "gcph_brand",
	pka_subbranddesc AS "gcph_subbrand",
	pka_variantdesc,
	pka_sizedesc AS "put_up_description",
	pka_skuiddesc AS "sku_type",
	pka_rootcode AS "pka_rootcode",
	pka_productdesc AS "pka_product_name",
	gcph_segment,
	gcph_subsegment,
	CASE 
		WHEN trans."mapping_matl_num" IS NULL
			OR trans."mapping_matl_num" = ''
			THEN trans.acommerce_item_sku
		ELSE trans."mapping_matl_num"
		END AS "matl_num",
	NULL AS "ean_upc",
	acommerce_item_sku AS "retailer_product_code",
	product_title AS "product_desc",
	brand,
	from_crncy,
	to_crncy,
	ex_rt,
	gmv AS "sales_value_lcy",
	gmv * ex_rt AS "sales_value_usd",
	quantity AS "sales_quantity",
	delivery_status
FROM trans
LEFT JOIN edw_calendar_dim calendar ON to_date(trans.marketplace_order_date) = calendar.cal_day::date
LEFT JOIN pka ON CASE 
		WHEN trans."mapping_matl_num" IS NULL
			OR trans."mapping_matl_num" = ''
			THEN trans.acommerce_item_sku
		ELSE trans."mapping_matl_num"
		END = pka."matl_num"
LEFT JOIN exch ON 'PHP' = exch.from_crncy
),
final as(
    select 
        "load_date"::timestamp_ntz(9) as load_date,
        filename::varchar(255) as filename,
        "market"::varchar(255) as market,
        "state"::varchar(30) as state,
        fisc_yr::number(18,0) as fisc_yr,
        fisc_per::number(18,0) as fisc_per,
        cal_yr::number(18,0) as cal_yr,
        cal_mo_2::number(18,0) as cal_mo_2,
        cal_day::date as cal_day,
        "transaction_date"::timestamp_ntz(9) as transaction_date,
        "order_date"::timestamp_ntz(9) as order_date,
        "order_id"::varchar(30) as order_id,
        "sap_customer_code"::varchar(30) as sap_customer_code,
        "customer_code"::varchar(30) as customer_code,
        "distributor_code"::varchar(30) as distributor_code,
        "distributor_name"::varchar(50) as distributor_name,
        "platform_name"::varchar(50) as platform_name,
        "retailer_name"::varchar(50) as retailer_name,
        "sub_store_name"::varchar(100) as sub_store_name,
        gcph_franchise::varchar(50) as gcph_franchise,
        "gcph_needstate"::varchar(50) as gcph_needstate,
        gcph_category::varchar(50) as gcph_category,
        gcph_subcategory::varchar(50) as gcph_subcategory,
        "gcph_brand"::varchar(50) as gcph_brand,
        "gcph_subbrand"::varchar(100) as gcph_subbrand,
        pka_variantdesc::varchar(50) as pka_variantdesc,
        "put_up_description"::varchar(100) as put_up_description,
        "sku_type"::varchar(50) as sku_type,
        "pka_rootcode"::varchar(70) as pka_rootcode,
        "pka_product_name"::varchar(255) as pka_product_name,
        gcph_segment::varchar(50) as gcph_segment,
        gcph_subsegment::varchar(100) as gcph_subsegment,
        "matl_num"::varchar(40) as matl_num,
        "ean_upc"::varchar(200) as ean_upc,
        "retailer_product_code"::varchar(20) as retailer_product_code,
        "product_desc"::varchar(400) as product_desc,
        brand::varchar(50) as brand,
        from_crncy::varchar(5) as from_crncy,
        to_crncy::varchar(5) as to_crncy,
        ex_rt::number(23,5) as ex_rt,
        "sales_value_lcy"::number(23,5) as sales_value_lcy,
        "sales_value_usd"::number(23,5) as sales_value_usd,
        "sales_quantity"::number(18,0) as sales_quantity,
        delivery_status::varchar(30) as delivery_status
    from transformed
)
select * from final