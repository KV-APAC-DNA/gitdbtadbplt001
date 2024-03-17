with
wks_thailand_base_detail as 
(
    select * from {{ source('snaposewks_integration', 'wks_thailand_base_detail') }}
),
thailand_propagate_from_to as
(
    select * from {{ ref('thawks_integration__thailand_propagate_from_to') }}
),
final as
(
select 
      p_from_to.dstr_nm::varchar(50) as dstr_nm,
      p_from_to.sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
      p_from_to.sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
      propagate_to::numeric(18, 0) as month,
      base.matl_num::varchar(50) as matl_num,
      base.so_qty::float as so_qty,
      base.so_value::float as so_value,
      base.inv_qty::float as inv_qty,
      base.inv_value::float as inv_value,
      base.sell_in_qty::numeric(38, 6) as sell_in_qty,
      base.sell_in_value::numeric(38, 5) as sell_in_value,
      base.last_3months_so::float as last_3months_so,
      base.last_6months_so::float as last_6months_so,
      base.last_12months_so::float as last_12months_so,
      base.last_3months_so_value::float as last_3months_so_value,
      base.last_6months_so_value::float as last_6months_so_value,
      base.last_12months_so_value::float as last_12months_so_value,
      base.last_36months_so_value::float as last_36months_so_value,
      'Y'::varchar(1) as propagation_flag,
      propagate_from::numeric(18, 0) as propagate_from,
      reason::varchar(29) as reason,
      base.replicated_flag::varchar(1) as replicated_flag
from  wks_thailand_base_detail base,
      thailand_propagate_from_to p_from_to
      where base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
      and upper(base.dstr_nm) = upper(p_from_to.dstr_nm)
      and base.month = p_from_to.propagate_from
)

select * from final