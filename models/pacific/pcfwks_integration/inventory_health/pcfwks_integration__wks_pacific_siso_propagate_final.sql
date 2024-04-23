with wks_pacific_base_detail as (
    select * from {{ ref('pcfwks_integration__wks_pacific_base_detail') }}
),
pacific_propagate_from_to as (
    select * from {{ ref('pcfwks_integration__pacific_propagate_from_to') }}
),
wks_pacific_siso_propagate_to_details as (
    select * from {{ ref('pcfwks_integration__wks_pacific_siso_propagate_to_details') }}
),
wks_pacific_siso_propagate_to_existing_dtls as (
    select * from {{ ref('pcfwks_integration__wks_pacific_siso_propagate_to_existing_dtls') }}
),
cte1 as (
    SELECT 
        sap_parent_customer_key,
        sap_parent_customer_desc,
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
    FROM wks_pacific_base_detail
    WHERE (sap_parent_customer_key, month) not in (
            select sap_parent_customer_key,
                propagate_to
            from pacific_propagate_from_to p_from_to
        )
),
cte2 as (
    select 
        propagated.sap_parent_customer_key,
        propagated.sap_parent_customer_desc,
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
        propagated.Propagation_Flag,
        cast (propagated.propagate_from as integer),
        propagated.reason,
        propagated.replicated_flag,
        existing.so_qty,
        existing.so_value,
        existing.inv_qty,
        existing.inv_value,
        existing.sell_in_qty,
        existing.sell_in_value,
        existing.last_3months_so,
        existing.last_6months_so,
        existing.last_12months_so,
        existing.last_3months_so_value,
        existing.last_6months_so_value,
        existing.last_12months_so_value
    from wks_pacific_siso_propagate_to_details propagated,
        wks_pacific_siso_propagate_to_existing_dtls existing
    where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
        and existing.matl_num = propagated.matl_num
        and existing.month = propagated.month
),
cte3 as (
    select 
        propagated.sap_parent_customer_key,
        propagated.sap_parent_customer_desc,
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
        propagated.Propagation_Flag,
        cast(propagated.propagate_from as integer),
        propagated.reason,
        replicated_flag,
        cast (null as numeric(38, 5)) so_qty,
        cast (null as numeric(38, 5)) so_value,
        cast (null as numeric(38, 5)) inv_qty,
        cast (null as numeric(38, 5)) inv_value,
        cast (null as numeric(38, 5)) sell_in_qty,
        cast (null as numeric(38, 5)) sell_in_value,
        cast (null as numeric(38, 5)) last_3months_so,
        cast (null as numeric(38, 5)) last_6months_so,
        cast (null as numeric(38, 5)) last_12months_so,
        cast (null as numeric(38, 5)) last_3months_so_value,
        cast (null as numeric(38, 5)) last_6months_so_value,
        cast (null as numeric(38, 5)) last_12months_so_value
    from wks_pacific_siso_propagate_to_details propagated
    where not exists (
            select 1
            from wks_pacific_siso_propagate_to_existing_dtls existing
            where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
                and existing.matl_num = propagated.matl_num
                and existing.month = propagated.month
        )
),
final as (
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
)
select * from final