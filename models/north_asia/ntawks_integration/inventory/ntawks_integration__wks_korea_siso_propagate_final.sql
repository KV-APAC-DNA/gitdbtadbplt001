with
wks_korea_siso_propagate_to_details as (
    select *
    from {{ ref('ntawks_integration__wks_korea_siso_propagate_to_details') }}
),
wks_korea_base_detail as (
    select *
    from {{ ref('ntawks_integration__wks_korea_base_detail') }}
),
wks_korea_siso_propagate_to_existing_dtls as (
    select *
    from {{ ref('ntawks_integration__wks_korea_siso_propagate_to_existing_dtls') }}
),
Korea_propagate_from_to as
(
    select * from {{ ref('ntawks_integration__korea_propagate_from_to') }}
),
wks_korea_siso_propagate_to_details as 
(
   select * from {{ ref('ntawks_integration__wks_korea_siso_propagate_to_details') }} 
),
temp_a as (
    SELECT sap_parent_customer_key,
        month,
        matl_num,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        sell_in_qty,
        sell_in_value,
        last_3months_so,
        last_6months_so,
        last_12months_so,
        last_3months_so_value,
        last_6months_so_value,
        last_12months_so_value,
        last_36months_so_value,
        'N' as propagate_flag,
        cast(null as integer) propagate_from,
        cast(null as varchar(100)) as reason,
        replicated_flag,
        cast (null as numeric(38, 5)) existing_so_qty,
        cast (null as numeric(38, 5)) existing_so_value,
        cast (null as numeric(38, 5)) existing_inv_qty,
        cast (null as numeric(38, 5)) existing_inv_value,
        cast (null as numeric(38, 5)) existing_sell_in_qty,
        cast (null as numeric(38, 5)) existing_sell_in_value,
        cast (null as numeric(38, 5)) existing_last_3months_so,
        cast (null as numeric(38, 5)) existing_last_6months_so,
        cast (null as numeric(38, 5)) existing_last_12months_so,
        cast (null as numeric(38, 5)) existing_last_3months_so_value,
        cast (null as numeric(38, 5)) existing_last_6months_so_value,
        cast (null as numeric(38, 5)) existing_last_12months_so_value
    FROM wks_korea_base_detail
    WHERE (sap_parent_customer_key, month) not in (
            select sap_parent_customer_key,
                propagate_to
            from Korea_propagate_from_to p_from_to
        )
),
temp_b as (
    select propagated.sap_parent_customer_key,
        propagated.month,
        propagated.matl_num,
        propagated.so_qty,
        propagated.so_value,
        propagated.inv_qty,
        propagated.inv_value,
        propagated.sell_in_qty,
        propagated.sell_in_value,
        propagated.last_3months_so,
        propagated.last_6months_so,
        propagated.last_12months_so,
        propagated.last_3months_so_value,
        propagated.last_6months_so_value,
        propagated.last_12months_so_value,
        propagated.last_36months_so_value,
        propagated.Propagation_Flag as propagate_flag,
        cast (propagated.propagate_from as integer) as PROPAGATE_FROM,
        propagated.reason,
        propagated.replicated_flag,
        existing.so_qty as existing_so_qty,
        existing.so_value as existing_so_value,
        existing.inv_qty as existing_inv_qty,
        existing.inv_value as existing_inv_value,
        existing.sell_in_qty as existing_sell_in_qty,
        existing.sell_in_value as existing_sell_in_value,
        existing.last_3months_so as existing_last_3months_so,
        existing.last_6months_so as existing_last_6months_so,
        existing.last_12months_so as existing_last_12months_so,
        existing.last_3months_so_value as existing_last_3months_so_value,
        existing.last_6months_so_value as existing_last_6months_so_value,
        existing.last_12months_so_value as existing_last_12months_so_value
    from wks_Korea_siso_propagate_to_details propagated,
        wks_Korea_siso_propagate_to_existing_dtls existing
    where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
        and existing.matl_num = propagated.matl_num
        and existing.month = propagated.month
),
final_b as (
    select sap_parent_customer_key,
        month,
        matl_num,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        sell_in_qty,
        sell_in_value,
        last_3months_so,
        last_6months_so,
        last_12months_so,
        last_3months_so_value,
        last_6months_so_value,
        last_12months_so_value,
        last_36months_so_value,
        propagate_flag,
        propagate_from,
        reason,
        replicated_flag,
        existing_so_qty,
        existing_so_value,
        existing_inv_qty,
        existing_inv_value,
        existing_sell_in_qty,
        existing_sell_in_value,
        existing_last_3months_so,
        existing_last_6months_so,
        existing_last_12months_so,
        existing_last_3months_so_value,
        existing_last_6months_so_value,
        existing_last_12months_so_value
    from temp_b
),
temp_c as (
    select propagated.sap_parent_customer_key,
        propagated.month,
        propagated.matl_num,
        propagated.so_qty,
        propagated.so_value,
        propagated.inv_qty,
        propagated.inv_value,
        propagated.sell_in_qty,
        propagated.sell_in_value,
        propagated.last_3months_so,
        propagated.last_6months_so,
        propagated.last_12months_so,
        propagated.last_3months_so_value,
        propagated.last_6months_so_value,
        propagated.last_12months_so_value,
        propagated.last_36months_so_value,
        propagated.Propagation_Flag as propagate_flag,
        cast(propagated.propagate_from as integer) as propagate_from,
        propagated.reason,
        replicated_flag,
              cast (null as numeric(38, 5)) existing_so_qty,
        cast (null as numeric(38, 5)) existing_so_value,
        cast (null as numeric(38, 5)) existing_inv_qty,
        cast (null as numeric(38, 5)) existing_inv_value,
        cast (null as numeric(38, 5)) existing_sell_in_qty,
        cast (null as numeric(38, 5)) existing_sell_in_value,
        cast (null as numeric(38, 5)) existing_last_3months_so,
        cast (null as numeric(38, 5)) existing_last_6months_so,
        cast (null as numeric(38, 5)) existing_last_12months_so,
        cast (null as numeric(38, 5)) existing_last_3months_so_value,
        cast (null as numeric(38, 5)) existing_last_6months_so_value,
        cast (null as numeric(38, 5)) existing_last_12months_so_value
    from wks_korea_siso_propagate_to_details propagated
    where not exists (
            select 1
            from wks_Korea_siso_propagate_to_existing_dtls existing
            where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
                and existing.matl_num = propagated.matl_num
                and existing.month = propagated.month
        )
),
final_c as (
    select sap_parent_customer_key,
        month,
        matl_num,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        sell_in_qty,
        sell_in_value,
        last_3months_so,
        last_6months_so,
        last_12months_so,
        last_3months_so_value,
        last_6months_so_value,
        last_12months_so_value,
        last_36months_so_value,
        propagate_flag,
        propagate_from,
        reason,
        replicated_flag,
        existing_so_qty,
        existing_so_value,
        existing_inv_qty,
        existing_inv_value,
        existing_sell_in_qty,
        existing_sell_in_value,
        existing_last_3months_so,
        existing_last_6months_so,
        existing_last_12months_so,
        existing_last_3months_so_value,
        existing_last_6months_so_value,
        existing_last_12months_so_value
    from temp_c
),
final as (
    select *
    from temp_a
    union all
    select *
    from final_b
    union all
    select *
    from final_c
)
select *
from final

