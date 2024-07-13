with wks_taiwan_base_detail as(
    select * from {{ ref('ntawks_integration__wks_taiwan_base_detail') }}
),
taiwan_propagate_from_to as(
    select * from {{ ref('ntawks_integration__taiwan_propagate_from_to') }}
),
final as
(   
    SELECT p_from_to.sap_parent_customer_key,
        p_from_to.sap_parent_customer_desc,
        p_from_to.bnr_key,
        p_from_to.bnr_desc,
        propagate_to as month,
        base.ean_num,
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
    FROM 
        wks_taiwan_base_detail base,
        taiwan_propagate_from_to p_from_to
    where base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
        and base.bnr_key = p_from_to.bnr_key
        and base.month = p_from_to.propagate_to
)
select * from final