with wks_my_lastnmonths as
(
    select * from {{ ref('myswks_integration__wks_my_lastnmonths') }}
),
wks_my_base as(
    select * from {{ source('myswks_integration', 'wks_my_base') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_mds_rg_tih_distributor_closure as
(
    select * from {{ ref('aspitg_integration__itg_mds_rg_tih_distributor_closure') }}
)
,
base as
(
    select
      distributor,
      dstrbtr_grp_cd,
      sap_parent_customer_key,
      sap_parent_customer_desc,
      matl_num,
      month,
      matl_num as base_matl_num,
      replicated_flag,
      sum(so_qty) as so_qty,
      sum(so_value) as so_value,
      sum(inv_qty) as inv_qty,
      sum(inv_value) as inv_value,
      sum(sell_in_qty) as sell_in_qty,
      sum(sell_in_value) as sell_in_value,
      sum(last_3months_so) as last_3months_so,
      sum(last_6months_so) as last_6months_so,
      sum(last_12months_so) as last_12months_so,
      sum(last_3months_so_value) as last_3months_so_value,
      sum(last_6months_so_value) as last_6months_so_value,
      sum(last_12months_so_value) as last_12months_so_value,
      sum(last_36months_so_value) as last_36months_so_value
    from (
      select
        agg.distributor,
        agg.dstrbtr_grp_cd,
        agg.sap_parent_customer_key,
        agg.sap_parent_customer_desc,
        agg.matl_num,
        agg.month,
        base.matl_num as base_matl_num,
        base.so_sls_qty as so_qty,
        base.so_trd_sls as so_value,
        base.inventory_quantity as inv_qty,
        base.inventory_val as inv_value,
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
          base.matl_num is null
        ) then 'Y' else 'N' end as replicated_flag
      from wks_my_lastnmonths as agg, wks_my_base as base
      where
        left(agg.month, 4) >= (
          date_part(year, '2024-01-18'::timestamp_ntz) - 2
        )
        and agg.distributor = base.distributor(+)
        and agg.dstrbtr_grp_cd = base.dstrbtr_grp_cd(+)
        and agg.sap_parent_customer_key = base.sap_prnt_cust_key(+)
        and agg.sap_parent_customer_desc = base.sap_prnt_cust_desc(+)
        and agg.matl_num = base.matl_num(+)
        and agg.month = base.mnth_id(+)
        and agg.month <= (
          select distinct
            cal_mnth_id as mnth_id
          from edw_vw_os_time_dim
          where
            cal_date = '2024-01-18'
        )
    )
    group by
      distributor,
      dstrbtr_grp_cd,
      sap_parent_customer_key,
      sap_parent_customer_desc,
      matl_num,
      month,
      replicated_flag
),
dist_closure as 
(
    select distinct
      dstrbtr_grp_cd,
      closemonth
    from itg_mds_rg_tih_distributor_closure
    where
      upper(market) = 'MALAYSIA'
),
base2 as
(
    select
        base.*,
        case
        when not dist_closure.dstrbtr_grp_cd is null and month > dist_closure.closemonth
        then 0
        else 1
        end as is_valid
    from base
    left join dist_closure
    on base.dstrbtr_grp_cd = dist_closure.dstrbtr_grp_cd
),
final as 
(
    select
        distributor,
        dstrbtr_grp_cd,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        matl_num,
        month,
        base_matl_num,
        replicated_flag,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        sell_in_qty,
        sell_in_value,
        last_3months_so,
        last_6months_so,
        last_12months_so,
        last_3months_so_value,
        last_6months_so_value,
        last_12months_so_value,
        last_36months_so_value
    from  base2
    where
    is_valid = 1
)
select 
    distributor::varchar(40) as distributor,
    dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    matl_num::varchar(100) as matl_num,
    month::number(18,0) as month,
    base_matl_num::varchar(100) as base_matl_num,
    replicated_flag::varchar(1) as replicated_flag,
    so_qty::number(38,6) as so_qty,
    so_value::number(38,17) as so_value,
    inv_qty::number(38,4) as inv_qty,
    inv_value::number(38,13) as inv_value,
    sell_in_qty::number(38,4) as sell_in_qty,
    sell_in_value::number(38,13) as sell_in_value,
    last_3months_so::number(38,6) as last_3months_so,
    last_6months_so::number(38,6) as last_6months_so,
    last_12months_so::number(38,6) as last_12months_so,
    last_3months_so_value::number(38,17) as last_3months_so_value,
    last_6months_so_value::number(38,17) as last_6months_so_value,
    last_12months_so_value::number(38,17) as last_12months_so_value,
    last_36months_so_value::number(38,17) as last_36months_so_value
 from final