with wks_india_base_detail as
(
  select * from ({{ ref('indwks_integration__wks_india_base_detail') }})
),
edw_vw_os_time_dim
as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
  ),
trans as
(
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
                WHERE cal_date = TRUNC(CONVERT_TIMEZONE('UTC', 'Asia/Singapore', CURRENT_TIMESTAMP), 'DAY')
                )
            )
          )
        WHERE mnth_id = (
            SELECT DISTINCT MNTH_ID
            FROM EDW_VW_OS_TIME_DIM
            WHERE cal_date = TRUNC(CONVERT_TIMEZONE('UTC', 'Asia/Singapore', CURRENT_TIMESTAMP), 'DAY')
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
  FROM wks_india_base_detail
  WHERE --sap_parent_customer_desc in ('COLES','WOOLWORTHS','METCASH','SYMBION','CENTRAL HEALTHCARE SERVICES PTY LTD','API','SIGMA') 
    --replicated_flag = 'N'
    month <= (
      SELECT DISTINCT cal_MNTH_ID
      FROM EDW_VW_OS_TIME_DIM
      WHERE cal_date = TRUNC(CONVERT_TIMEZONE('UTC', 'Asia/Singapore', CURRENT_TIMESTAMP), 'DAY')
      )
  GROUP BY sap_parent_customer_key,
    month
  )
),
final as
(
    select
    sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
	month::number(38,0) as month,
	so_qty::number(38,4) as so_qty,
	so_value::number(38,3) as so_value,
	inv_qty::number(38,3) as inv_qty,
	inv_value::number(38,6) as inv_value,
	propagate_flag::varchar(1) as propagate_flag,
	latest_month::number(38,0) as latest_month
    from trans
)
select * from final
  