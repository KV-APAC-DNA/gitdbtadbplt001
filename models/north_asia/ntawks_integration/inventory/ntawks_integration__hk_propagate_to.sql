WITH EDW_VW_OS_TIME_DIM
AS (
	SELECT *
	FROM DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
	),
wks_HK_base_detail
AS (
	SELECT *
	FROM DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_BASE_DETAIL
	),
transformed
AS (
	SELECT sap_parent_customer_key,
		month,
		so_qty,
		so_value,
		inv_qty,
		inv_value,
		CASE 
			WHEN month > (
					SELECT -- mnth_id,
						third_month
					FROM (
						SELECT mnth_id,
							LAG(mnth_id, 2) OVER (
								ORDER BY mnth_id
								) third_month
						FROM (
							SELECT DISTINCT "year",
								mnth_id
							FROM EDW_VW_OS_TIME_DIM
							WHERE mnth_id <= (
									SELECT DISTINCT MNTH_ID
									FROM EDW_VW_OS_TIME_DIM
									WHERE cal_date = to_date(convert_timezone('Asia/Singapore', convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)))
									)
							)
						)
					WHERE mnth_id = (
							SELECT DISTINCT MNTH_ID
							FROM EDW_VW_OS_TIME_DIM
							WHERE cal_date = to_date(convert_timezone('Asia/Singapore', convert_timezone('UTC', current_timestamp())::timestamp_ntz(9)))
							)
					)
				AND (
					nvl(so_value, 0) = 0
					OR nvl(inv_value, 0) = 0
					)
				THEN 'Y'
			ELSE 'N'
			END AS propagate_flag,
		max(month) OVER (PARTITION BY sap_parent_customer_key) latest_month
	FROM (
		SELECT sap_parent_customer_key,
			month,
			sum(so_qty) so_qty,
			sum(so_value) so_value,
			sum(inv_qty) inv_qty,
			sum(inv_value) inv_value
		FROM wks_HK_base_detail
		WHERE --sap_parent_customer_desc in ('COLES','WOOLWORTHS','METCASH','SYMBION','CENTRAL HEALTHCARE SERVICES PTY LTD','API','SIGMA') 
			replicated_flag = 'N'
		GROUP BY sap_parent_customer_key,
			month
		)
	),
final
AS (
	SELECT sap_parent_customer_key::VARCHAR(50) AS sap_parent_customer_key,
		month::VARCHAR(23) AS month,
		so_qty::number(38, 0) AS so_qty,
		so_value::number(38, 4) AS so_value,
		inv_qty::number(38, 5) AS inv_qty,
		inv_value::number(38, 9) AS inv_value,
		propagate_flag::VARCHAR(1) AS propagate_flag,
		latest_month::VARCHAR(23) AS latest_month
	FROM transformed
	)
SELECT *
FROM final
