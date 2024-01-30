with wks_my_base_detail as
(
    select * from {{ ref('myswks_integration__wks_my_base_detail') }}
),
my_propagate_from_to as
(
    select * from {{ ref('myswks_integration__my_propagate_from_to') }}
),
final as
(select
  p_from_to.distributor,
  p_from_to.dstrbtr_grp_cd,
  p_from_to.sap_parent_customer_key,
  p_from_to.sap_parent_customer_desc,
  propagate_to as month,
  base.matl_num,
  base.so_qty,
  base.so_value,
  base.inv_qty,
  base.inv_value,
  base.sell_in_qty,
  base.sell_in_value,
  base.last_3months_so,
  base.last_6months_so,
  base.last_12months_so,
  base.last_3months_so_value,
  base.last_6months_so_value,
  base.last_12months_so_value,
  base.last_36months_so_value,
  'Y' as propagation_flag,
  propagate_from,
  reason,
  base.replicated_flag
from wks_my_base_detail as base, my_propagate_from_to as p_from_to
where
  base.distributor = p_from_to.distributor
  and base.dstrbtr_grp_cd = p_from_to.dstrbtr_grp_cd
  and base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
  and base.month = p_from_to.propagate_from)
select * from final
