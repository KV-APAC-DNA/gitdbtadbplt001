with indonesia_propagate_from_to as
(
    select * from {{ ref('idnwks_integration__indonesia_propagate_from_to') }}
),
wks_indonesia_base_detail as
(
    select * from {{ ref('idnwks_integration__wks_indonesia_base_detail') }}
),
final as 
(
    SELECT p_from_to.jj_sap_dstrbtr_nm,
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
           'Y' as Propagation_Flag,
           propagate_from,
           reason,
           base.replicated_flag
        FROM wks_indonesia_base_detail  base,indonesia_propagate_from_to p_from_to
        where  base.jj_sap_dstrbtr_nm = p_from_to.jj_sap_dstrbtr_nm
        and base.sap_parent_customer_key = p_from_to.sap_parent_customer_key 
        and base.sap_parent_customer_desc = p_from_to.sap_parent_customer_desc
        and base.month = p_from_to.propagate_from
)
select * from final
