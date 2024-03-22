with edw_vw_os_time_dim as(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_thailand_lastnmonths as (
select * from {{ ref('thawks_integration__wks_thailand_lastnmonths') }}
),
wks_thailand_base as (
select * from {{ ref('thawks_integration__wks_thailand_base') }}
),
trans as(
  select
    agg.dstr_nm,
    agg.sap_parent_customer_key,
    agg.sap_parent_customer_desc,
    agg.matl_num,
    agg.month,
    base.sku_cd as base_matl_num,
    base.so_sls_qty as so_qty,
    base.so_grs_trd_sls as so_val,
    base.inventory_quantity as closing_stock,
    base.inventory_val as closing_stock_val,
    base.si_sls_qty as sell_in_qty,
    base.si_gts_val as sell_in_value,
    agg.last_3months_so,
    agg.last_6months_so,
    agg.last_12months_so,
    agg.last_3months_so_value,
    agg.last_6months_so_value,
    agg.last_12months_so_value,
    agg.last_36months_so_value,
    case when (
      base.sku_cd is null
    ) then 'Y' else 'N' end as replicated_flag
  from wks_thailand_lastnmonths as agg, wks_thailand_base as base
  where
    left(agg.month, 4) >= (
      date_part(year, current_timestamp()) - 2
    )
    and agg.sap_parent_customer_key = base.sap_prnt_cust_key(+)
    and upper(agg.sap_parent_customer_desc) = upper(base.sap_prnt_cust_desc(+))
    and upper(agg.dstr_nm) = upper(base.dstr_nm(+))
    and agg.matl_num = base.sku_cd(+)
    and agg.month = base.cal_mnth_id(+)
    and agg.month <= (
      select distinct
        cal_mnth_id as mnth_id
      from edw_vw_os_time_dim
      where
        cal_date = current_date()
    
		)
),
final as (

select
  dstr_nm,
  sap_parent_customer_key,
  sap_parent_customer_desc,
  matl_num,
  month,
  matl_num as base_matl_num,
  replicated_flag,
  sum(so_qty) as so_qty,
  sum(so_val) as so_value,
  sum(closing_stock) as inv_qty,
  sum(closing_stock_val) as inv_value,
  sum(sell_in_qty) as sell_in_qty,
  sum(sell_in_value) as sell_in_value,
  sum(last_3months_so) as last_3months_so,
  sum(last_6months_so) as last_6months_so,
  sum(last_12months_so) as last_12months_so,
  sum(last_3months_so_value) as last_3months_so_value,
  sum(last_6months_so_value) as last_6months_so_value,
  sum(last_12months_so_value) as last_12months_so_value,
  sum(last_36months_so_value) as last_36months_so_value
from trans
group by
  dstr_nm,
  sap_parent_customer_key,
  sap_parent_customer_desc,
  matl_num,
  month,
  replicated_flag
)

select * from final