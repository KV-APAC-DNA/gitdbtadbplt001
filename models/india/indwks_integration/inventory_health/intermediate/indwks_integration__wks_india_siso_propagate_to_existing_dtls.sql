with wks_india_base_detail as
(
    select * from ({{ ref('indwks_integration__wks_india_base_detail') }})
),
india_propagate_from_to as
(
    select * from ({{ ref('indwks_integration__india_propagate_from_to') }})
),
trans as
(
    SELECT p_from_to.sap_parent_customer_key,
      propagate_to AS month,
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
      propagate_from,
      base.replicated_flag
    FROM wks_india_base_detail base,
      india_propagate_from_to p_from_to
    WHERE base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
      AND base.month = p_from_to.propagate_to
),
final as
  (
    select
        sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
	    month::number(38,0) as month,
	    matl_num::varchar(50) as matl_num,
	    so_qty::number(38,4) as so_qty,
	    so_value::number(38,3) as so_value,
	    inv_qty::number(38,3) as inv_qty,
	    inv_value::number(38,6) as inv_value,
	    sell_in_qty::number(38,4) as sell_in_qty,
	    sell_in_value::number(38,4) as sell_in_value,
	    last_3months_so::number(38,4) as last_3months_so,
	    last_6months_so::number(38,4) as last_6months_so,
	    last_12months_so::number(38,4) as last_12months_so,
	    last_3months_so_value::number(38,3) as last_3months_so_value,
	    last_6months_so_value::number(38,3) as last_6months_so_value,
	    last_12months_so_value::number(38,3) as last_12months_so_value,
	    last_36months_so_value::number(38,3) as last_36months_so_value,
	    propagate_from::number(38,0) as propagate_from,
	    replicated_flag::varchar(1) as replicated_flag
    from trans
  )
  select * from final