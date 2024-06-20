with wks_taiwan_base_detail as
(
    select * from snapntawks_integration.wks_taiwan_base_detail
),
taiwan_propagate_from_to as
(
    select * from snapntawks_integration.taiwan_propagate_from_to
),
wks_taiwan_siso_propagate_to_details as
(
    select * from snapntawks_integration.wks_taiwan_siso_propagate_to_details
),
wks_taiwan_siso_propagate_to_existing_dtls as
(
    select * from snapntawks_integration.wks_taiwan_siso_propagate_to_existing_dtls
),
union_1 as
(
    SELECT
        sap_parent_customer_key,
        sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        month,
        ean_num,
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
    FROM wks_taiwan_base_detail
    WHERE (sap_parent_customer_key, bnr_key, month) not in
        (
            select sap_parent_customer_key,
                bnr_key,
                propagate_to
            from taiwan_propagate_from_to p_from_to
        )
),
union_2 as
(
    select
        propagated.sap_parent_customer_key,
        propagated.sap_parent_customer_desc,
        propagated.bnr_key,
        propagated.bnr_desc,
        propagated.month,
        propagated.ean_num,
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
        cast (propagated.propagate_from as integer),
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
    from wks_taiwan_siso_propagate_to_details propagated,
        wks_taiwan_siso_propagate_to_existing_dtls existing
    where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
        and existing.bnr_key = propagated.bnr_key
        and existing.ean_num = propagated.ean_num
        and existing.month = propagated.month
),
union_3 as
(
    select
        propagated.sap_parent_customer_key,
        propagated.sap_parent_customer_desc,
        propagated.bnr_key,
        propagated.bnr_desc,
        propagated.month,
        propagated.ean_num,
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
        cast(propagated.propagate_from as integer),
        propagated.reason,
        replicated_flag,
        cast (null as numeric(38, 5)) as existing_so_qty,
        cast (null as numeric(38, 5)) as existing_so_value,
        cast (null as numeric(38, 5)) as existing_inv_qty,
        cast (null as numeric(38, 5)) as existing_inv_value,
        cast (null as numeric(38, 5)) as existing_sell_in_qty,
        cast (null as numeric(38, 5)) as existing_sell_in_value,
        cast (null as numeric(38, 5)) as existing_last_3months_so,
        cast (null as numeric(38, 5)) as existing_last_6months_so,
        cast (null as numeric(38, 5)) as existing_last_12months_so,
        cast (null as numeric(38, 5)) as existing_last_3months_so_value,
        cast (null as numeric(38, 5)) as existing_last_6months_so_value,
        cast (null as numeric(38, 5)) as existing_last_12months_so_value
    from wks_taiwan_siso_propagate_to_details propagated
    where not exists
        (
            select 1
            from wks_taiwan_siso_propagate_to_existing_dtls existing
            where existing.sap_parent_customer_key = propagated.sap_parent_customer_key
                and existing.bnr_key = propagated.bnr_key
                and existing.ean_num = propagated.ean_num
                and existing.month = propagated.month
        )
),
final as
(
    select
        sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
        sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
        bnr_key::varchar(12) as bnr_key,
        bnr_desc::varchar(50) as bnr_desc,
        month::number(18,0) as month,
        ean_num::varchar(100) as ean_num,
        so_qty::number(38,4) as so_qty,
        so_value::number(38,4) as so_value,
        inv_qty::number(38,5) as inv_qty,
        inv_value::number(38,9) as inv_value,
        sell_in_qty::number(38,4) as sell_in_qty,
        sell_in_value::number(38,4) as sell_in_value,
        last_3months_so::number(38,4) as last_3months_so,
        last_6months_so::number(38,4) as last_6months_so,
        last_12months_so::number(38,4) as last_12months_so,
        last_3months_so_value::number(38,4) as last_3months_so_value,
        last_6months_so_value::number(38,4) as last_6months_so_value,
        last_12months_so_value::number(38,4) as last_12months_so_value,
        last_36months_so_value::number(38,4) as last_36months_so_value,
        propagate_flag::varchar(1) as propagate_flag,
        propagate_from::number(18,0) as propagate_from,
        reason::varchar(100) as reason,
        replicated_flag::varchar(1) as replicated_flag,
        existing_so_qty::number(38,5) as existing_so_qty,
        existing_so_value::number(38,5) as existing_so_value,
        existing_inv_qty::number(38,5) as existing_inv_qty,
        existing_inv_value::number(38,5) as existing_inv_value,
        existing_sell_in_qty::number(38,5) as existing_sell_in_qty,
        existing_sell_in_value::number(38,5) as existing_sell_in_value,
        existing_last_3months_so::number(38,5) as existing_last_3months_so,
        existing_last_6months_so::number(38,5) as existing_last_6months_so,
        existing_last_12months_so::number(38,5) as existing_last_12months_so,
        existing_last_3months_so_value::number(38,5) as existing_last_3months_so_value,
        existing_last_6months_so_value::number(38,5) as existing_last_6months_so_value,
        existing_last_12months_so_value::number(38,5) as existing_last_12months_so_value
    from
    (
        select * from union_1
        union all
        select * from union_2
        union all
        select * from union_3
    )

)
select * from final