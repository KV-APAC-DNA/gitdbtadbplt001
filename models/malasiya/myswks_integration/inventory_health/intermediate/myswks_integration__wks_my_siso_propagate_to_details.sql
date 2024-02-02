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
  and base.month = p_from_to.propagate_from
)
select 
    distributor::varchar(40) as distributor,
    dstrbtr_grp_cd::varchar(30) as dstrbtr_grp_cd,
    sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
    sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
    month::number(18,0) as month,
    matl_num::varchar(100) as matl_num,
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
    last_36months_so_value::number(38,17) as last_36months_so_value,
    propagation_flag::varchar(1) as propagation_flag,
    propagate_from::number(18,0) as propagate_from,
    reason::varchar(29) as reason,
    replicated_flag::varchar(1) as replicated_flag
from final
