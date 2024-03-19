with edw_vw_os_time_dim as(
select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_thailand_base as (
select * from {{ ref('thawks_integration__wks_thailand_base') }}
),
all_months as (
  select distinct
    dstr_nm,
    sap_parent_customer_key,
    sap_parent_customer_desc,
    matl_num,
    b.mnth_id
  from (
    select distinct
      dstr_nm,
      sap_prnt_cust_key as sap_parent_customer_key,
      sap_prnt_cust_desc as sap_parent_customer_desc,
      sku_cd as matl_num
    from wks_thailand_base
  ) as a, (
    select distinct
      cal_year as year,
      cal_mnth_id as mnth_id
    from edw_vw_os_time_dim /* limit 100 */
    where
      cal_year >= (
        date_part(year, current_timestamp()) - 6
      )
  ) as b
),
transformed as(
select
  all_months.dstr_nm,
  all_months.sap_parent_customer_key,
  all_months.sap_parent_customer_desc,
  all_months.matl_num,
  all_months.mnth_id as month,
  sum(b.so_sls_qty) as so_qty,
  sum(b.so_grs_trd_sls) as so_value,
  sum(b.inventory_quantity) as inv_qty,
  sum(b.inventory_val) as inv_value,
  sum(b.si_sls_qty) as sell_in_qty,
  sum(b.si_gts_val) as sell_in_value
from all_months, wks_thailand_base as b
where
  all_months.sap_parent_customer_key = b.sap_prnt_cust_key(+)
  and all_months.sap_parent_customer_desc = b.sap_prnt_cust_desc(+)
  and all_months.matl_num = b.sku_cd(+)
  and all_months.mnth_id = b.cal_mnth_id(+)
  and upper(all_months.dstr_nm) = upper(b.dstr_nm(+))
group by
  all_months.dstr_nm,
  all_months.sap_parent_customer_key,
  all_months.sap_parent_customer_desc,
  all_months.matl_num,
  all_months.mnth_id
 )
select * from transformed