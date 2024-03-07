with 
wks_th_watsons_propagate_from_to as(
    select * from {{ ref('thawks_integration__wks_th_watsons_propagate_from_to') }}
),
wks_th_watsons_base_detail as(
    select * from {{ ref('thawks_integration__wks_th_watsons_base_detail') }}
),
final as 
(
    select
        p_from_to.sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
        p_from_to.sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
        propagate_to::numeric(18,0) as month,
        base.month::numeric(18,0) as base_month,
        base.matl_num::varchar(1500) as matl_num,
        base.so_qty::numeric(38,4) as so_qty,
        base.so_value::numeric(38,8) as so_value,
        base.inv_qty::numeric(38,4) as inv_qty,
        base.inv_value::numeric(38,8) as inv_value,
        base.sell_in_qty::numeric(38,4) as sell_in_qty,
        base.sell_in_value::numeric(38,4) as sell_in_value,
        base.last_3months_so::numeric(38,4) as last_3months_so,
        base.last_6months_so::numeric(38,4) as last_6months_so,
        base.last_12months_so::numeric(38,4) as last_12months_so,
        base.last_3months_so_value::numeric(38,8) as last_3months_so_value,
        base.last_6months_so_value::numeric(38,8) as last_6months_so_value,
        base.last_12months_so_value::numeric(38,8) as last_12months_so_value,
        base.last_15months_so_value::numeric(38,8) as last_15months_so_value,
        base.last_18months_so_value::numeric(38,8) as last_18months_so_value,
        base.last_21months_so_value::numeric(38,8) as last_21months_so_value,
        base.last_24months_so_value::numeric(38,8) as last_24months_so_value,
        base.last_27months_so_value::numeric(38,8) as last_27months_so_value,
        base.last_30months_so_value::numeric(38,8) as last_30months_so_value,
        base.last_33months_so_value::numeric(38,8) as last_33months_so_value,
        base.last_36months_so_value::numeric(38,8) as last_36months_so_value,
        'Y'::varchar(1) as propagation_flag,
        propagate_from::numeric(18,0) as propagate_from,
        reason::varchar(29) as reason,
        base.replicated_flag::varchar(1) as replicated_flag
    from wks_th_watsons_base_detail as base, wks_th_watsons_propagate_from_to as p_from_to
    where
        base.sap_parent_customer_key = p_from_to.sap_parent_customer_key
        and base.month = p_from_to.propagate_from
)

select * from final

