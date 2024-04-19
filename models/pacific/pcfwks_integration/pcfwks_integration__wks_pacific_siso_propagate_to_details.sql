with 
pacific_propagate_from_to as(
    select * from {{ ref('pcfwks_integration__pacific_propagate_from_to') }}
),
wks_pacific_base_detail as(
    select * from {{ ref('pcfwks_integration__wks_pacific_base_detail') }}
),
final as
(
select 
    p_from_to.sap_parent_customer_key::varchar(20) as sap_parent_customer_key,
    p_from_to.sap_parent_customer_desc::varchar(75) as sap_parent_customer_desc,
    propagate_to::varchar(23) as month,
    base.matl_num::varchar(100) as matl_num,
    base.so_qty::number(38,4) as so_qty,
    base.so_value::number(38,4) as so_value,
    base.inv_qty::number(38,5) as inv_qty,
    base.inv_value::number(38,4) as inv_value,
    base.sell_in_qty::number(38,5) as sell_in_qty,
    base.sell_in_value::number(38,5) as sell_in_value,
    base.last_3months_so::number(38,4) as last_3months_so,
    base.last_6months_so::number(38,4) as last_6months_so,
    base.last_12months_so::number(38,4) as last_12months_so,
    base.last_3months_so_value::number(38,4) as last_3months_so_value,
    base.last_6months_so_value::number(38,4) as last_6months_so_value,
    base.last_12months_so_value::number(38,4) as last_12months_so_value,
    base.last_36months_so_value::number(38,4) as last_36months_so_value,
    'Y'::varchar(1) as propagation_flag,
    propagate_from::varchar(23) as propagate_from,
    reason::varchar(29) as reason,
    base.replicated_flag::varchar(1) as replicated_flag 
from wks_pacific_base_detail base,pacific_propagate_from_to p_from_to
where base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
    and base.month = p_from_to.propagate_from
)
select * from final