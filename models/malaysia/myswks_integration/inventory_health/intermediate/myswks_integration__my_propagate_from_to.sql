with my_propagate_to as
(
    select * from {{ ref('myswks_integration__my_propagate_to') }}
),
wks_my_base_detail as
(
    select * from {{ ref('myswks_integration__wks_my_base_detail') }}
),
p_to as
(
    select
    *,
    case
        when propagate_flag = 'Y'
        and (coalesce(so_value, 0) = 0 and coalesce(inv_value, 0) = 0)
        then 'Sellout and Inventory Missing'
        when propagate_flag = 'Y' and coalesce(so_value, 0) = 0
        then 'Sellout Missing'
        when propagate_flag = 'Y' and coalesce(inv_value, 0) = 0
        then 'Inventory Missing'
        else 'not propagate'
    end as reason
    from my_propagate_to
    where
    propagate_flag = 'Y'
),
all_months as
(
    select
        distributor,
        dstrbtr_grp_cd,
        sap_parent_customer_key,
        sap_parent_customer_desc,
        month
    from wks_my_base_detail
    where
    not (distributor, dstrbtr_grp_cd, sap_parent_customer_key, month) in 
    (
    select
        distributor,
        dstrbtr_grp_cd,
        sap_parent_customer_key,
        month
    from my_propagate_to
    where
        propagate_flag = 'Y'
    )
    group by
    distributor,
    dstrbtr_grp_cd,
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month
    having (sum(so_qty) > 0 and sum(inv_qty) > 0)
),
max_month as
(
    select
      distributor,
      dstrbtr_grp_cd,
      sap_parent_customer_key,
      sap_parent_customer_desc,
      max(month) as month
    from  all_months
    group by
      distributor,
      dstrbtr_grp_cd,
      sap_parent_customer_key,
      sap_parent_customer_desc
),

base as
(
  select
    d.distributor,
    d.dstrbtr_grp_cd,
    d.sap_parent_customer_key,
    d.sap_parent_customer_desc,
    d.month as month,
    sum(so_qty) as so_qty,
    sum(so_value) as so_value,
    sum(inv_qty) as inv_qty,
    sum(inv_value) as inv_value
  from  max_month, wks_my_base_detail as d
  where
    max_month.distributor = d.distributor
    and max_month.dstrbtr_grp_cd = d.dstrbtr_grp_cd
    and max_month.sap_parent_customer_key = d.sap_parent_customer_key
    and max_month.month = d.month
  group by
    d.distributor,
    d.dstrbtr_grp_cd,
    d.sap_parent_customer_key,
    d.sap_parent_customer_desc,
    d.month
),
final as
(
    select
        p_to.distributor,
        p_to.dstrbtr_grp_cd,
        p_to.sap_parent_customer_key,
        p_to.sap_parent_customer_desc,
        p_to.latest_month,
        p_to.month as propagate_to,
        base.month as propagate_from,
        base.so_qty,
        base.inv_qty,
        datediff(month,to_date(base.month||'01', 'yyyymmdd'), to_date(latest_month||'01', 'yyyymmdd')) as diff_month,
        p_to.reason
    from  p_to, base
    where
    p_to.distributor = base.distributor
    and p_to.dstrbtr_grp_cd = base.dstrbtr_grp_cd
    and p_to.sap_parent_customer_key = base.sap_parent_customer_key
    and base.month < p_to.month
    and datediff(month,to_date(base.month||'01', 'yyyymmdd'), to_date(latest_month||'01', 'yyyymmdd')) <= 2
)
select 
    distributor::varchar(40) as distributor,
    dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    latest_month::number(18,0) as latest_month,
    propagate_to::number(18,0) as propagate_to,
    propagate_from::number(18,0) as propagate_from,
    so_qty::number(38,6) as so_qty,
    inv_qty::number(38,4) as inv_qty,
    diff_month::number(38,0) as diff_month,
    reason::varchar(29) as reason
from final
