with edw_key_account_dim as(
    select * from {{ ref('indedw_integration__edw_key_account_dim') }}
),
v_product_dim as (
    select * from {{ ref('indedw_integration__v_product_dim') }}
),
edw_retailer_calendar_dim as (
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
edw_ka_sales_fact as (
    select * from {{ ref('indedw_integration__edw_ka_sales_fact') }}
),
edw_user_dim as (
    select * from {{ ref('indedw_integration__edw_user_dim') }}
),
union1 as(
	SELECT CASE 
		WHEN kdd.parent_code IS NULL
			OR RTRIM(kdd.parent_code::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE kdd.parent_code
		END AS parent_code,
	CASE 
		WHEN kdd.ka_name IS NULL
			OR RTRIM(kdd.ka_name::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE kdd.ka_name
		END AS ka_name,
	'Direct'::CHARACTER VARYING AS ka_type,
	CASE 
		WHEN ksfd.product_code IS NULL
			OR RTRIM(ksfd.product_code::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE ksfd.product_code
		END AS product_code,
	CASE 
		WHEN kdd.parent_name IS NULL
			OR RTRIM(kdd.parent_name::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE kdd.parent_name
		END AS parent_name,
	cd.fisc_yr,
	cd.mth_yyyymm,
	cd.day,
	CASE 
		WHEN ksfd.totalsalesnrconfirmed IS NULL
			THEN 0::NUMERIC::NUMERIC(18, 0)
		ELSE ksfd.totalsalesnrconfirmed
		END AS totalsalesnrconfirmed,
	ksfd.customer_code FROM edw_ka_sales_fact ksfd LEFT JOIN (
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
	--WHERE edw_key_account_dim.active_flag = 'Y'::char AND
	WHERE edw_key_account_dim.ka_flag = 'D'::char
	) kdd ON ksfd.customer_code::TEXT = kdd.ka_code::TEXT LEFT JOIN v_product_dim pd ON ksfd.product_code::TEXT = pd.product_code::TEXT LEFT JOIN edw_retailer_calendar_dim cd ON ksfd.invoice_date = cd.day LEFT JOIN edw_user_dim ud ON kdd.region_name::TEXT = ud.region_name::TEXT
	AND kdd.zone_name::TEXT = ud.zone_name::TEXT
	AND kdd.territory_name::TEXT = ud.territory_name::TEXT WHERE ksfd.retailer_code IS NULL
	OR RTRIM(ksfd.retailer_code::TEXT) = ''
),
union2 as(
	SELECT CASE 
		WHEN kdw.parent_code IS NULL
			OR RTRIM(kdw.parent_code::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE kdw.parent_code
		END AS parent_code,
	CASE 
		WHEN ksfw.retailer_name IS NULL
			OR RTRIM(ksfw.retailer_name::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE ksfw.retailer_name
		END AS ka_name,
	'Wholesaler'::CHARACTER VARYING AS ka_type,
	CASE 
		WHEN ksfw.product_code IS NULL
			OR RTRIM(ksfw.product_code::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE ksfw.product_code
		END AS product_code,
	CASE 
		WHEN kdw.parent_name IS NULL
			OR RTRIM(kdw.parent_name::TEXT) = ''
			THEN 'Unknown'::CHARACTER VARYING
		ELSE kdw.parent_name
		END AS parent_name,
	cd.fisc_yr,
	cd.mth_yyyymm,
	cd.day,
	CASE 
		WHEN ksfw.totalsalesnrconfirmed IS NULL
			THEN 0::NUMERIC::NUMERIC(18, 0)
		ELSE ksfw.totalsalesnrconfirmed
		END AS totalsalesnrconfirmed,
	ksfw.customer_code FROM edw_ka_sales_fact ksfw LEFT JOIN (
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
	WHERE edw_key_account_dim.ka_flag = 'W'::char
		--  AND   edw_key_account_dim.active_flag = 'Y'::bpchar
	) kdw ON ksfw.retailer_code::TEXT = kdw.ka_code::TEXT
	AND ksfw.customer_code::TEXT = kdw.distributor_code::TEXT LEFT JOIN v_product_dim pd ON ksfw.product_code::TEXT = pd.product_code::TEXT LEFT JOIN edw_retailer_calendar_dim cd ON ksfw.invoice_date = cd.day WHERE ksfw.retailer_code IS NOT NULL
	AND RTRIM(ksfw.retailer_code::TEXT) <> ''
),
final as(
    select * from union1
    union all
    select * from union2
)
select * from final