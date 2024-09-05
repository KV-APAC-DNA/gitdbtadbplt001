with wks_Japan_base as(
    select * from {{ ref('jpnwks_integration__wks_japan_base') }}
),
EDW_VW_OS_TIME_DIM as(
    select * from sgpedw_integration.EDW_VW_OS_TIME_DIM
),
final as(
    select
  all_months.sap_parent_customer_key,
  all_months.sap_parent_customer_desc,
  all_months.matl_num,
  all_months.mnth_id as month,
  sum(b.so_qty) as so_qty,
  sum(b.so_val) as so_value,
  sum(b.closing_stock) as inv_qty,
  sum(b.closing_stock_val) as inv_value,
  sum(b.si_sls_qty) as sell_in_qty,
  sum(b.si_gts_val) as sell_in_value
from
  (
    select
      distinct sap_parent_customer_key,
      sap_parent_customer_desc,
      matl_num,
      b.mnth_id
    from
      (
        select
          distinct prnt_key as sap_parent_customer_key,
          prnt_cust_desc as sap_parent_customer_desc,
          matl_num
        from
          wks_Japan_base
      ) a,
      (
        select
          distinct cal_year as year,
          cal_mnth_id as mnth_id
        from
          edw_vw_os_time_dim
        where
          cal_year >= (date_part(year, convert_timezone('UTC', current_timestamp())) -2)
      ) b
  ) all_months,
  wks_Japan_base b
where
  all_months.sap_parent_customer_key = b.prnt_key (+)
  and all_months.sap_parent_customer_desc = b.prnt_cust_desc (+)
  and all_months.matl_num = b.matl_num (+)
  and all_months.mnth_id = b.mnth_id(+) 
group by
  all_months.sap_parent_customer_key,
  all_months.sap_parent_customer_desc,
  all_months.matl_num,
  all_months.mnth_id
)
select * from final