{{ config(
  sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
) }}

with edw_key_account_dim as
(
    select * from {{ ref('indedw_integration__edw_key_account_dim') }}
),
edw_ka_sales_fact as
(
    select * from {{ ref('indedw_integration__edw_ka_sales_fact') }}
),
v_product_dim as
(
    select * from {{ ref('indedw_integration__v_product_dim') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
kdd as 
(
	SELECT edw_key_account_dim.ka_code,
		edw_key_account_dim.ka_name,
		edw_key_account_dim.ka_flag,
		edw_key_account_dim.parent_code,
		edw_key_account_dim.parent_name,
		edw_key_account_dim.distributor_code,
		edw_key_account_dim.distributor_name,
		edw_key_account_dim.ka_address1,
		edw_key_account_dim.ka_address2,
		edw_key_account_dim.ka_address3,
		edw_key_account_dim.region_code,
		edw_key_account_dim.region_name,
		edw_key_account_dim.zone_code,
		edw_key_account_dim.zone_name,
		edw_key_account_dim.territory_code,
		edw_key_account_dim.territory_name,
		edw_key_account_dim.state_code,
		edw_key_account_dim.state_name,
		edw_key_account_dim.town_code,
		edw_key_account_dim.town_name,
		edw_key_account_dim.active_flag,
		edw_key_account_dim.abi_code,
		edw_key_account_dim.abi_name,
		edw_key_account_dim.plant,
		edw_key_account_dim.crt_dttm,
		edw_key_account_dim.updt_dttm
	FROM edw_key_account_dim edw_key_account_dim
	WHERE (edw_key_account_dim.ka_flag = 'D'::char)
),
cte1 as
(
	SELECT CASE 
			WHEN (
					(kdd.parent_code IS NULL)
					OR (trim((kdd.parent_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.parent_code
			END AS parent_code,
		CASE 
			WHEN (
					(ksfd.customer_code IS NULL)
					OR (trim((ksfd.customer_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfd.customer_code
			END AS customer_code,
		CASE 
			WHEN (
					(ksfd.customer_code IS NULL)
					OR (trim((ksfd.customer_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfd.customer_code
			END AS ka_code,
		CASE 
			WHEN (
					(kdd.ka_name IS NULL)
					OR (trim((kdd.ka_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.ka_name
			END AS ka_name,
		'Direct' AS ka_type,
		CASE 
			WHEN (
					(kdd.distributor_name IS NULL)
					OR (trim((kdd.distributor_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.distributor_name
			END AS distributor_name,
		CASE 
			WHEN (
					(kdd.parent_name IS NULL)
					OR (trim((kdd.parent_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.parent_name
			END AS parent_name,
		CASE 
			WHEN (
					(kdd.region_name IS NULL)
					OR (trim((kdd.region_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.region_name
			END AS region_name,
		CASE 
			WHEN (
					(kdd.zone_name IS NULL)
					OR (trim((kdd.zone_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.zone_name
			END AS zone_name,
		CASE 
			WHEN (
					(kdd.territory_name IS NULL)
					OR (trim((kdd.territory_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.territory_name
			END AS territory_name,
		CASE 
			WHEN (
					(kdd.state_name IS NULL)
					OR (trim((kdd.state_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.state_name
			END AS state_name,
		CASE 
			WHEN (
					(kdd.town_name IS NULL)
					OR (trim((kdd.town_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.town_name
			END AS town_name,
		CASE 
			WHEN (
					(kdd.plant IS NULL)
					OR (trim((kdd.plant)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.plant
			END AS plant,
		CASE 
			WHEN (
					(kdd.abi_name IS NULL)
					OR (trim((kdd.abi_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdd.abi_name
			END AS abi_name,
		CASE 
			WHEN (
					(ksfd.product_code IS NULL)
					OR (trim((ksfd.product_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfd.product_code
			END AS product_code,
		CASE 
			WHEN (
					(pd.product_name IS NULL)
					OR (trim((pd.product_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.product_name
			END AS product_name,
		CASE 
			WHEN (
					(pd.franchise_name IS NULL)
					OR (trim((pd.franchise_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.franchise_name
			END AS franchise_name,
		CASE 
			WHEN (
					(pd.brand_name IS NULL)
					OR (trim((pd.brand_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.brand_name
			END AS brand_name,
		CASE 
			WHEN (
					(pd.product_category_name IS NULL)
					OR (trim((pd.product_category_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.product_category_name
			END AS product_category_name,
		CASE 
			WHEN (
					(pd.variant_name IS NULL)
					OR (trim((pd.variant_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.variant_name
			END AS variant_name,
		CASE 
			WHEN (
					(pd.mothersku_name IS NULL)
					OR (trim((pd.mothersku_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.mothersku_name
			END AS mothersku_name,
		cd.day,
		CASE 
			WHEN (cd.mth_yyyymm = 1)
				THEN 'JANUARY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 2)
				THEN 'FEBRUARY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 3)
				THEN 'MARCH'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 4)
				THEN 'APRIL'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 5)
				THEN 'MAY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 6)
				THEN 'JUNE'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 7)
				THEN 'JULY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 8)
				THEN 'AUGUST'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 9)
				THEN 'SEPTEMBER'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 10)
				THEN 'OCTOBER'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 11)
				THEN 'NOVEMBER'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 12)
				THEN 'DECEMBER'::CHARACTER VARYING
			ELSE 'UNKNOWN'::CHARACTER VARYING
			END AS month,
		((('Q'::CHARACTER VARYING)::TEXT || ((cd.qtr)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS qtr,
		((('Week '::CHARACTER VARYING)::TEXT || ((cd.week)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS week,
		cd.fisc_yr AS year,
		CASE 
			WHEN (ksfd.prdqty IS NULL)
				THEN 0
			ELSE ksfd.prdqty
			END AS prdqty,
		CASE 
			WHEN (ksfd.prdtaxamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.prdtaxamt
			END AS prdtaxamt,
		CASE 
			WHEN (ksfd.prdschdiscamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.prdschdiscamt
			END AS prdschdiscamt,
		CASE 
			WHEN (ksfd.prddbdiscamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.prddbdiscamt
			END AS prddbdiscamt,
		CASE 
			WHEN (ksfd.salwdsamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.salwdsamt
			END AS salwdsamt,
		CASE 
			WHEN (ksfd.totalgrosssalesincltax IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.totalgrosssalesincltax
			END AS totalgrosssalesincltax,
		CASE 
			WHEN (ksfd.totalsalesnr IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.totalsalesnr
			END AS totalsalesnr,
		CASE 
			WHEN (ksfd.totalsalesconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.totalsalesconfirmed
			END AS totalsalesconfirmed,
		CASE 
			WHEN (ksfd.totalsalesnrconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.totalsalesnrconfirmed
			END AS totalsalesnrconfirmed,
		CASE 
			WHEN (ksfd.totalsalesunconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.totalsalesunconfirmed
			END AS totalsalesunconfirmed,
		CASE 
			WHEN (ksfd.totalsalesnrunconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfd.totalsalesnrunconfirmed
			END AS totalsalesnrunconfirmed,
		CASE 
			WHEN (ksfd.totalqtyconfirmed IS NULL)
				THEN 0
			ELSE ksfd.totalqtyconfirmed
			END AS totalqtyconfirmed,
		CASE 
			WHEN (ksfd.totalqtyunconfirmed IS NULL)
				THEN 0
			ELSE ksfd.totalqtyunconfirmed
			END AS totalqtyunconfirmed,
		CASE 
			WHEN (
					(ksfd.buyingoutlets IS NULL)
					OR (trim((ksfd.buyingoutlets)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfd.buyingoutlets
			END AS totalbuyingoutlets,
		null AS abi_ntid,
		null AS flm_ntid,
		null AS bdm_ntid,
		null AS rsm_ntid
	FROM (
		(
			(
				edw_ka_sales_fact ksfd LEFT JOIN kdd ON (((ksfd.customer_code)::TEXT = (kdd.ka_code)::TEXT))
				) LEFT JOIN v_product_dim pd ON (((ksfd.product_code)::TEXT = (pd.product_code)::TEXT))
			) LEFT JOIN edw_retailer_calendar_dim cd ON ((ksfd.invoice_date = cd.day))
		)
	WHERE (
			(ksfd.retailer_code IS NULL)
			OR (trim((ksfd.retailer_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
			)
),
kdw as
(
	SELECT edw_key_account_dim.ka_code,
		edw_key_account_dim.ka_name,
		edw_key_account_dim.ka_flag,
		edw_key_account_dim.parent_code,
		edw_key_account_dim.parent_name,
		edw_key_account_dim.distributor_code,
		edw_key_account_dim.distributor_name,
		edw_key_account_dim.ka_address1,
		edw_key_account_dim.ka_address2,
		edw_key_account_dim.ka_address3,
		edw_key_account_dim.region_code,
		edw_key_account_dim.region_name,
		edw_key_account_dim.zone_code,
		edw_key_account_dim.zone_name,
		edw_key_account_dim.territory_code,
		edw_key_account_dim.territory_name,
		edw_key_account_dim.state_code,
		edw_key_account_dim.state_name,
		edw_key_account_dim.town_code,
		edw_key_account_dim.town_name,
		edw_key_account_dim.active_flag,
		edw_key_account_dim.abi_code,
		edw_key_account_dim.abi_name,
		edw_key_account_dim.plant,
		edw_key_account_dim.crt_dttm,
		edw_key_account_dim.updt_dttm
	FROM edw_key_account_dim edw_key_account_dim
	WHERE (edw_key_account_dim.ka_flag = 'W'::char)
),
cte2 as
(
	SELECT CASE 
			WHEN (
					(kdw.parent_code IS NULL)
					OR (trim((kdw.parent_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.parent_code
			END AS parent_code,
		CASE 
			WHEN (
					(ksfw.customer_code IS NULL)
					OR (trim((ksfw.customer_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfw.customer_code
			END AS customer_code,
		CASE 
			WHEN (
					(ksfw.retailer_code IS NULL)
					OR (trim((ksfw.retailer_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfw.retailer_code
			END AS ka_code,
		CASE 
			WHEN (
					(ksfw.retailer_name IS NULL)
					OR (trim((ksfw.retailer_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfw.retailer_name
			END AS ka_name,
		'Wholesaler' AS ka_type,
		CASE 
			WHEN (
					(kdw.distributor_name IS NULL)
					OR (trim((kdw.distributor_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.distributor_name
			END AS distributor_name,
		CASE 
			WHEN (
					(kdw.parent_name IS NULL)
					OR (trim((kdw.parent_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.parent_name
			END AS parent_name,
		CASE 
			WHEN (
					(kdw.region_name IS NULL)
					OR (trim((kdw.region_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.region_name
			END AS region_name,
		CASE 
			WHEN (
					(kdw.zone_name IS NULL)
					OR (trim((kdw.zone_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.zone_name
			END AS zone_name,
		CASE 
			WHEN (
					(kdw.territory_name IS NULL)
					OR (trim((kdw.territory_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.territory_name
			END AS territory_name,
		CASE 
			WHEN (
					(kdw.state_name IS NULL)
					OR (trim((kdw.state_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.state_name
			END AS state_name,
		CASE 
			WHEN (
					(kdw.town_name IS NULL)
					OR (trim((kdw.town_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.town_name
			END AS town_name,
		CASE 
			WHEN (
					(kdw.plant IS NULL)
					OR (trim((kdw.plant)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.plant
			END AS plant,
		CASE 
			WHEN (
					(kdw.abi_name IS NULL)
					OR (trim((kdw.abi_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE kdw.abi_name
			END AS abi_name,
		CASE 
			WHEN (
					(ksfw.product_code IS NULL)
					OR (trim((ksfw.product_code)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfw.product_code
			END AS product_code,
		CASE 
			WHEN (
					(pd.product_name IS NULL)
					OR (trim((pd.product_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.product_name
			END AS product_name,
		CASE 
			WHEN (
					(pd.franchise_name IS NULL)
					OR (trim((pd.franchise_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.franchise_name
			END AS franchise_name,
		CASE 
			WHEN (
					(pd.brand_name IS NULL)
					OR (trim((pd.brand_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.brand_name
			END AS brand_name,
		CASE 
			WHEN (
					(pd.product_category_name IS NULL)
					OR (trim((pd.product_category_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.product_category_name
			END AS product_category_name,
		CASE 
			WHEN (
					(pd.variant_name IS NULL)
					OR (trim((pd.variant_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.variant_name
			END AS variant_name,
		CASE 
			WHEN (
					(pd.mothersku_name IS NULL)
					OR (trim((pd.mothersku_name)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE pd.mothersku_name
			END AS mothersku_name,
		cd.day,
		CASE 
			WHEN (cd.mth_yyyymm = 1)
				THEN 'JANUARY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 2)
				THEN 'FEBRUARY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 3)
				THEN 'MARCH'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 4)
				THEN 'APRIL'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 5)
				THEN 'MAY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 6)
				THEN 'JUNE'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 7)
				THEN 'JULY'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 8)
				THEN 'AUGUST'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 9)
				THEN 'SEPTEMBER'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 10)
				THEN 'OCTOBER'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 11)
				THEN 'NOVEMBER'::CHARACTER VARYING
			WHEN (cd.mth_yyyymm = 12)
				THEN 'DECEMBER'::CHARACTER VARYING
			ELSE 'UNKNOWN'::CHARACTER VARYING
			END AS month,
		((('Q'::CHARACTER VARYING)::TEXT || ((cd.qtr)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS qtr,
		((('Week '::CHARACTER VARYING)::TEXT || ((cd.week)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS week,
		cd.fisc_yr AS year,
		CASE 
			WHEN (ksfw.prdqty IS NULL)
				THEN 0
			ELSE ksfw.prdqty
			END AS prdqty,
		CASE 
			WHEN (ksfw.prdtaxamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.prdtaxamt
			END AS prdtaxamt,
		CASE 
			WHEN (ksfw.prdschdiscamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.prdschdiscamt
			END AS prdschdiscamt,
		CASE 
			WHEN (ksfw.prddbdiscamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.prddbdiscamt
			END AS prddbdiscamt,
		CASE 
			WHEN (ksfw.salwdsamt IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.salwdsamt
			END AS salwdsamt,
		CASE 
			WHEN (ksfw.totalgrosssalesincltax IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.totalgrosssalesincltax
			END AS totalgrosssalesincltax,
		CASE 
			WHEN (ksfw.totalsalesnr IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.totalsalesnr
			END AS totalsalesnr,
		CASE 
			WHEN (ksfw.totalsalesconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.totalsalesconfirmed
			END AS totalsalesconfirmed,
		CASE 
			WHEN (ksfw.totalsalesnrconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.totalsalesnrconfirmed
			END AS totalsalesnrconfirmed,
		CASE 
			WHEN (ksfw.totalsalesunconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.totalsalesunconfirmed
			END AS totalsalesunconfirmed,
		CASE 
			WHEN (ksfw.totalsalesnrunconfirmed IS NULL)
				THEN ((0)::NUMERIC)::NUMERIC(18, 0)
			ELSE ksfw.totalsalesnrunconfirmed
			END AS totalsalesnrunconfirmed,
		CASE 
			WHEN (ksfw.totalqtyconfirmed IS NULL)
				THEN 0
			ELSE ksfw.totalqtyconfirmed
			END AS totalqtyconfirmed,
		CASE 
			WHEN (ksfw.totalqtyunconfirmed IS NULL)
				THEN 0
			ELSE ksfw.totalqtyunconfirmed
			END AS totalqtyunconfirmed,
		CASE 
			WHEN (
					(ksfw.buyingoutlets IS NULL)
					OR (trim((ksfw.buyingoutlets)::TEXT) = (''::CHARACTER VARYING)::TEXT)
					)
				THEN 'Unknown'::CHARACTER VARYING
			ELSE ksfw.buyingoutlets
			END AS totalbuyingoutlets,
		null AS abi_ntid,
		null AS flm_ntid,
		null AS bdm_ntid,
		null AS rsm_ntid
	FROM (
		(
			(
				edw_ka_sales_fact ksfw LEFT JOIN kdw ON (
						(
							((ksfw.retailer_code)::TEXT = (kdw.ka_code)::TEXT)
							AND ((ksfw.customer_code)::TEXT = (kdw.distributor_code)::TEXT)
							)
						)
				) LEFT JOIN v_product_dim pd ON (((ksfw.product_code)::TEXT = (pd.product_code)::TEXT))
			) LEFT JOIN edw_retailer_calendar_dim cd ON ((ksfw.invoice_date = cd.day))
		)
	WHERE (
			(ksfw.retailer_code IS NOT NULL)
			AND (trim((ksfw.retailer_code)::TEXT) <> (''::CHARACTER VARYING)::TEXT)
			)
),
final as
(
	select * from cte1
	UNION ALL
	select * from cte2
)
select * from final
