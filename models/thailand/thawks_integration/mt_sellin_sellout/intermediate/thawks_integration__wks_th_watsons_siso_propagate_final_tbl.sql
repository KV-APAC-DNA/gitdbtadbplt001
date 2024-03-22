with wks_th_watsons_siso_propagate_final as (
  select * from {{ ref('thawks_integration__wks_th_watsons_siso_propagate_final') }}
),
edw_vw_os_time_dim as (
  select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
transformed as (
select
  sap_parent_customer_key,
  sap_parent_customer_desc,
  matl_num,
  a.month,
  t.wks,
  inv_qty,
  inv_value,
  sell_in_qty,
  sell_in_value,
  (
    so_qty * wks
  ) as so_qty,
  (
    so_value * wks
  ) as so_value,
  last_3months_so,
  last_6months_so,
  last_12months_so,
  (
    so_qty * 13
  ) as so_13wks,
  (
    so_value * 13
  ) as so_value_13wks,
  (
    so_26_wks * 26
  ) as so_26_wks,
  (
    so_value_26_wks * 26
  ) as so_value_26,
  (
    so_54_wks * 52
  ) as so_54_wks,
  (
    so_value_54_wks * 52
  ) as so_value_54_wks,
  (
    so_value_l36_mnth * 156
  ) as so_value_l36_mnth,
  propagate_flag,
  propagate_from,
  reason,
  replicated_flag
from (
  select
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    matl_num,
    propagate_flag,
    propagate_from,
    reason,
    replicated_flag,
    sum(inv_qty) as inv_qty,
    sum(inv_value) as inv_value,
    sum(sell_in_qty) as sell_in_qty,
    sum(sell_in_value) as sell_in_value,
    sum(so_qty) as so_qty,
    sum(so_value) as so_value,
    sum(last_3months_so) as last_3months_so,
    sum(last_6months_so) as last_6months_so,
    sum(last_12months_so) as last_12months_so,
    sum(so_26_wks / 2) as so_26_wks,
    sum(so_value_26_wks / 2) as so_value_26_wks,
    sum(so_54_wks / 4) as so_54_wks,
    sum(so_value_54_wks / 4) as so_value_54_wks,
    sum(so_value_l36_mnth / 12) as so_value_l36_mnth
  from (
    select
      sap_parent_customer_key,
      sap_parent_customer_desc,
      month,
      matl_num,
      inv_qty,
      inv_value,
      sell_in_qty,
      sell_in_value,
      so_qty,
      so_value,
      last_3months_so,
      last_6months_so,
      last_12months_so,
      last_3months_so_value,
      last_6months_so_value,
      last_12months_so_value,
      propagate_flag,
      propagate_from,
      reason,
      replicated_flag,
      case
        when last_3months_so is null
        then sum(coalesce(so_qty, 0))
        else cast(sum(
          coalesce(cast(so_qty as decimal(38, 2)), 0) + coalesce(cast(last_3months_so as decimal(38, 4)), 0)
        ) as decimal(38, 4))
      end as so_26_wks,
      case
        when last_3months_so_value is null
        then sum(coalesce(so_value, 0))
        else cast(sum(coalesce(so_value, 0) + coalesce(last_3months_so_value, 0)) as decimal(38, 4))
      end as so_value_26_wks,
      case
        when (
          last_3months_so is null and last_6months_so is null and last_12months_so is null
        )
        then so_qty
        when (
          last_6months_so is null and last_12months_so is null
        )
        then cast(sum(coalesce(so_qty, 0) + coalesce(last_3months_so, 0)) as decimal(38, 4))
        when last_12months_so is null
        then cast(sum(
          coalesce(so_qty, 0) + coalesce(last_3months_so, 0) + coalesce(last_6months_so, 0)
        ) as decimal(38, 4))
        else cast(sum(
          coalesce(so_qty, 0) + coalesce(last_3months_so, 0) + coalesce(last_6months_so, 0) + coalesce(last_12months_so, 0)
        ) as decimal(38, 4))
      end as so_54_wks,
      case
        when (
          last_3months_so_value is null
          and last_6months_so_value is null
          and last_12months_so_value is null
        )
        then so_value
        when (
          last_6months_so_value is null and last_12months_so_value is null
        )
        then cast(sum(coalesce(so_value, 0) + coalesce(last_3months_so_value, 0)) as decimal(38, 4))
        when last_12months_so_value is null
        then cast(sum(
          coalesce(so_value, 0) + coalesce(last_3months_so_value, 0) + coalesce(last_6months_so_value, 0)
        ) as decimal(38, 4))
        else cast(sum(
          coalesce(so_value, 0) + coalesce(last_3months_so_value, 0) + coalesce(last_6months_so_value, 0) + coalesce(last_12months_so_value, 0)
        ) as decimal(38, 4))
      end as so_value_54_wks,
      cast(sum(
        coalesce(so_value, 0) + coalesce(last_3months_so_value, 0) + coalesce(last_6months_so_value, 0) + coalesce(last_12months_so_value, 0) + coalesce(last_15months_so_value, 0) + coalesce(last_18months_so_value, 0) + coalesce(last_21months_so_value, 0) + coalesce(last_24months_so_value, 0) + coalesce(last_27months_so_value, 0) + coalesce(last_30months_so_value, 0) + coalesce(last_33months_so_value, 0) + coalesce(last_36months_so_value, 0)
      ) as decimal(38, 4)) as so_value_l36_mnth
    from wks_th_watsons_siso_propagate_final
    group by
      sap_parent_customer_key,
      sap_parent_customer_desc,
      month,
      matl_num,
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
      propagate_flag,
      propagate_from,
      reason,
      replicated_flag
  )
  where
    left(month, 4) >= (date_part(year, current_timestamp()) - 6)
  group by
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    matl_num,
    propagate_flag,
    propagate_from,
    reason,
    replicated_flag
) as a
left join (
  select distinct
    mnth_id,
    count(distinct wk) as wks
  from edw_vw_os_time_dim
  group by
    mnth_id
) as t
  on a.month = t.mnth_id
),
final as (
    select
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    matl_num::varchar(1500) as matl_num,
    month::number(18,0) as month,
    wks::number(38,0) as wks,
    inv_qty::number(38,4) as inv_qty,
    inv_value::number(38,8) as inv_value,
    sell_in_qty::number(38,4) as sell_in_qty,
    sell_in_value::number(38,4) as sell_in_value,
    so_qty::number(38,4) as so_qty,
    so_value::number(38,8) as so_value,
    last_3months_so::number(38,4) as last_3months_so,
    last_6months_so::number(38,4) as last_6months_so,
    last_12months_so::number(38,4) as last_12months_so,
    so_13wks::number(38,4) as so_13wks,
    so_value_13wks::number(38,8) as so_value_13wks,
    so_26_wks::number(38,4) as so_26_wks,
    so_value_26::number(38,8) as so_value_26,
    so_54_wks::number(38,4) as so_54_wks,
    so_value_54_wks::number(38,8) as so_value_54_wks,
    so_value_l36_mnth::number(38,4) as so_value_l36_mnth,
    propagate_flag::varchar(1) as propagate_flag,
    propagate_from::number(18,0) as propagate_from,
    reason::varchar(100) as reason,
    replicated_flag::varchar(1) as replicated_flag
    from transformed
)

select * from final