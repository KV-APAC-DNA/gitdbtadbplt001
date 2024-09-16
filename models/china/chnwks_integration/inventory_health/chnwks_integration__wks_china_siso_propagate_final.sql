with wks_china_base_detail_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_base_detail_inv') }}
),
china_propagate_from_to as
(
    select * from {{ ref('chnwks_integration__china_propagate_from_to') }}
),
wks_china_siso_propagate_to_details as
(
    select * from {{ ref('chnwks_integration__wks_china_siso_propagate_to_details') }}
),
wks_china_siso_propagate_to_existing_dtls as
(
    select * from {{ ref('chnwks_integration__wks_china_siso_propagate_to_existing_dtls') }}
),
union1 as
(
    SELECT 
    country_name, 
    sold_to, 
    sap_prnt_cust_key, 
    sap_bnr_key, 
    bu, 
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
    cast(
        null as varchar(100)
    ) as reason, 
    replicated_flag, 
    cast (
        null as numeric(38, 5)
    ) existing_so_qty, 
    cast (
        null as numeric(38, 5)
    ) existing_so_value, 
    cast (
        null as numeric(38, 5)
    ) existing_inv_qty, 
    cast (
        null as numeric(38, 5)
    ) existing_inv_value, 
    cast (
        null as numeric(38, 5)
    ) existing_sell_in_qty, 
    cast (
        null as numeric(38, 5)
    ) existing_sell_in_value, 
    cast (
        null as numeric(38, 5)
    ) existing_last_3months_so, 
    cast (
        null as numeric(38, 5)
    ) existing_last_6months_so, 
    cast (
        null as numeric(38, 5)
    ) existing_last_12months_so, 
    cast (
        null as numeric(38, 5)
    ) existing_last_3months_so_value, 
    cast (
        null as numeric(38, 5)
    ) existing_last_6months_so_value, 
    cast (
        null as numeric(38, 5)
    ) existing_last_12months_so_value 
    FROM 
    wks_china_base_detail_inv 
    WHERE 
    (
        country_name, sold_to, sap_prnt_cust_key, 
        sap_bnr_key, bu, month
    ) not in (
        select 
        country_name, 
        sold_to, 
        sap_prnt_cust_key, 
        sap_bnr_key, 
        bu, 
        propagate_to 
        from 
        china_propagate_from_to p_from_to
    )
),
union2 as
(
    select 
    propagated.country_name, 
    propagated.sold_to, 
    propagated.sap_prnt_cust_key, 
    propagated.sap_bnr_key, 
    propagated.bu, 
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
    cast (
        propagated.propagate_from as integer
    ), 
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
    from 
    wks_china_siso_propagate_to_details propagated, 
    wks_china_siso_propagate_to_existing_dtls existing 
    where 
    --existing.sap_prnt_cust_desc = propagated.sap_prnt_cust_desc  
    existing.country_name = propagated.country_name 
    and existing.sap_prnt_cust_key = propagated.sap_prnt_cust_key 
    and existing.sold_to = propagated.sold_to 
    and existing.sap_bnr_key = propagated.sap_bnr_key 
    and existing.bu = propagated.bu 
    and existing.matl_num = propagated.matl_num 
    and existing.month = propagated.month
),
union3 as
(
    select 
    propagated.country_name, 
    propagated.sold_to, 
    propagated.sap_prnt_cust_key, 
    propagated.sap_bnr_key, 
    propagated.bu, 
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
    cast(
        propagated.propagate_from as integer
    ), 
    propagated.reason, 
    replicated_flag, 
    cast (
        null as numeric(38, 5)
    ) so_qty, 
    cast (
        null as numeric(38, 5)
    ) so_value, 
    cast (
        null as numeric(38, 5)
    ) inv_qty, 
    cast (
        null as numeric(38, 5)
    ) inv_value, 
    cast (
        null as numeric(38, 5)
    ) sell_in_qty, 
    cast (
        null as numeric(38, 5)
    ) sell_in_value, 
    cast (
        null as numeric(38, 5)
    ) last_3months_so, 
    cast (
        null as numeric(38, 5)
    ) last_6months_so, 
    cast (
        null as numeric(38, 5)
    ) last_12months_so, 
    cast (
        null as numeric(38, 5)
    ) last_3months_so_value, 
    cast (
        null as numeric(38, 5)
    ) last_6months_so_value, 
    cast (
        null as numeric(38, 5)
    ) last_12months_so_value 
    from 
    wks_china_siso_propagate_to_details propagated 
    where 
    not exists (
        select 
        1 
        from 
        wks_china_siso_propagate_to_existing_dtls existing 
        where 
        existing.country_name = propagated.country_name 
        and existing.sold_to = propagated.sold_to 
        and existing.sap_prnt_cust_key = propagated.sap_prnt_cust_key 
        and existing.bu = propagated.bu 
        and existing.matl_num = propagated.matl_num 
        and existing.month = propagated.month
    )
),
final as
(
    select * from union1
    UNION ALL
    select * from union2
    UNION ALL
    select * from union3
)
select * from final