with wks_vietnam_base_detail as (
    select * from dev_dna_core.SNAPOSEWKS_INTEGRATION.wks_vietnam_base_detail
),
vietnam_propagate_from_to as (
    select * from dev_dna_core.SNAPOSEWKS_INTEGRATION.vietnam_propagate_from_to
),
final as (
    SELECT p_from_to.sap_parent_customer_key,
    p_from_to.sap_parent_customer_desc,
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
    'Y' AS Propagation_Flag,
    propagate_from,
    reason,
    base.replicated_flag
FROM wks_vietnam_base_detail base,
    vietnam_propagate_from_to p_from_to
WHERE base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
    AND base.month = p_from_to.propagate_from
)
select * from final