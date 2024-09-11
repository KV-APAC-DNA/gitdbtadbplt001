with wks_japan_base_detail as(
    select * from {{ ref('jpnwks_integration__wks_japan_base_detail') }}
),
japan_propagate_from_to as(
    select * from {{ ref('jpwks_integration__japan_propagate_from_to') }}
),
final as(
    SELECT p_from_to.sap_parent_customer_key,
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
       propagate_from ,
       base.replicated_flag
FROM wks_japan_base_detail  base
     ,japan_propagate_from_to p_from_to
where base.sap_parent_customer_key = p_from_to.sap_parent_customer_key 
  and base.month = p_from_to.propagate_to
)
select * from final
