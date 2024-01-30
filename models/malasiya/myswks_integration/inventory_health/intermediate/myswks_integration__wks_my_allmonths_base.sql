with wks_my_base as
(
    select * from {{ source('myswks_integration', 'wks_my_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
a as
(
    select distinct
      distributor,
      dstrbtr_grp_cd,
      sap_prnt_cust_key,
      sap_prnt_cust_desc,
      matl_num
    from wks_my_base
),
b as(
    select distinct
        cal_year as year,
        cal_mnth_id as month
    from edw_vw_os_time_dim 
    where
        cal_year >= (
        date_part(year, current_timestamp()) - 6
        )
),
all_months as
(
  select distinct
    distributor,
    dstrbtr_grp_cd,
    sap_prnt_cust_key,
    sap_prnt_cust_desc,
    matl_num,
    month
  from a,  b
)
final as
    (select
    all_months.distributor,
    all_months.dstrbtr_grp_cd,
    all_months.sap_prnt_cust_key,
    all_months.sap_prnt_cust_desc,
    all_months.matl_num,
    all_months.month as month,
    sum(b.so_sls_qty) as so_qty,
    sum(b.so_trd_sls) as so_value,
    sum(b.inventory_quantity) as inv_qty,
    sum(b.inventory_val) as inv_value,
    sum(b.si_sls_qty) as sell_in_qty,
    sum(b.si_gts_val) as sell_in_value
    from all_months, wks_my_base as b
    where
    all_months.distributor = b.distributor(+)
    and all_months.dstrbtr_grp_cd = b.dstrbtr_grp_cd(+)
    and all_months.sap_prnt_cust_key = b.sap_prnt_cust_key(+)
    and all_months.sap_prnt_cust_desc = b.sap_prnt_cust_desc(+)
    and all_months.matl_num = b.matl_num(+)
    and month = mnth_id(+)
    group by
    all_months.distributor,
    all_months.dstrbtr_grp_cd,
    all_months.sap_prnt_cust_key,
    all_months.sap_prnt_cust_desc,
    all_months.matl_num,
    all_months.month
)
select * from final

