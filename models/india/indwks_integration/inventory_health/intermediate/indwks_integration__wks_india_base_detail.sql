with wks_india_lastnmonths as(
  select * from ({{ ref('indwks_integration__wks_india_lastnmonths') }})
),
wks_india_base as
(
  select * from ({{ ref('indwks_integration__wks_india_base') }})
),
edw_vw_os_time_dim
as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
  ),
trans as
(
SELECT sap_parent_customer_key,
  matl_num,
  month,
  matl_num AS base_matl_num,
  replicated_flag,
  sum(so_qty) so_qty,
  sum(so_value) so_value,
  sum(inv_qty) inv_qty,
  sum(inv_value) inv_value,
  sum(sell_in_qty) sell_in_qty,
  sum(sell_in_value) sell_in_value,
  sum(last_3months_so) last_3months_so,
  sum(last_6months_so) last_6months_so,
  sum(last_12months_so) last_12months_so,
  sum(last_3months_so_value) last_3months_so_value,
  sum(last_6months_so_value) last_6months_so_value,
  sum(last_12months_so_value) last_12months_so_value,
  sum(last_36months_so_value) last_36months_so_value
FROM (
  SELECT agg.sap_parent_customer_key,
    agg.matl_num,
    agg.month,
    base.matl_num AS base_matl_num,
    base.so_qty AS so_qty,
    base.so_val AS so_value,
    base.closing_stock AS inv_qty,
    base.closing_stock_val AS inv_value,
    base.si_sls_qty AS sell_in_qty,
    base.si_gts_val AS sell_in_value,
    agg.last_3months_so,
    agg.last_6months_so,
    agg.last_12months_so,
    agg.last_3months_so_value,
    agg.last_6months_so_value,
    agg.last_12months_so_value,
    agg.last_36months_so_value,
    CASE 
      WHEN (base.matl_num IS NULL)
        THEN 'Y'
      ELSE 'N'
      END AS replicated_flag
  FROM wks_india_lastnmonths agg,
    wks_india_base base
  WHERE LEFT(agg.month, 4) >= (date_part(year, convert_timezone('UTC', current_timestamp())) -2)
    AND agg.sap_parent_customer_key = base.sold_to(+)
    AND agg.matl_num = base.matl_num(+)
    AND agg.month = base.mnth_id(+)
    AND agg.month <= (
      SELECT DISTINCT mnth_id AS mnth_id
      FROM edw_vw_os_time_dim
      WHERE cal_date = TRUNC(CURRENT_DATE, 'DAY')
      )
  )
GROUP BY sap_parent_customer_key,
  matl_num,
  month,
  replicated_flag
  ),
  final as
  (
    select
    sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
	  matl_num::varchar(50) as matl_num,
	  month::number(38,0) as month,
	  base_matl_num::varchar(50) as base_matl_num,
	  replicated_flag::varchar(1) as replicated_flag,
	  so_qty::number(38,4) as so_qty,
	  so_value::number(38,3) as so_value,
	  inv_qty::number(38,3) as inv_qty,
	  inv_value::number(38,6) as inv_value,
	  sell_in_qty::number(38,4) as sell_in_qty,
	  sell_in_value::number(38,4) as sell_in_value,
	  last_3months_so::number(38,4) as last_3months_so,
	  last_6months_so::number(38,4) as last_6months_so,
	  last_12months_so::number(38,4) as last_12months_so,
	  last_3months_so_value::number(38,3) as last_3months_so_value,
	  last_6months_so_value::number(38,3) as last_6months_so_value,
	  last_12months_so_value::number(38,3) as last_12months_so_value,
	  last_36months_so_value::number(38,3) as last_36months_so_value
    from trans
  )
  select * from final