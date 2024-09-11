with wks_china_base_detail_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_base_detail_inv') }}
),
china_propagate_from_to as
(
    select * from {{ ref('chnwks_integration__china_propagate_from_to') }}
),
final as 
(
    SELECT 
    p_from_to.country_name, 
    p_from_to.sold_to, 
    p_from_to.sap_prnt_cust_key, 
    p_from_to.sap_bnr_key, 
    p_from_to.bu, 
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
    'Y' as Propagation_Flag, 
    propagate_from, 
    reason, 
    base.replicated_flag 
    FROM 
    wks_china_base_detail_inv base, 
    china_propagate_from_to p_from_to 
    where 
    base.country_name = p_from_to.country_name 
    and base.sap_prnt_cust_key = p_from_to.sap_prnt_cust_key 
    and base.sold_to = p_from_to.sold_to 
    and base.sap_bnr_key = p_from_to.sap_bnr_key 
    and base.bu = p_from_to.bu 
    and base.month = p_from_to.propagate_from
)
select * from final