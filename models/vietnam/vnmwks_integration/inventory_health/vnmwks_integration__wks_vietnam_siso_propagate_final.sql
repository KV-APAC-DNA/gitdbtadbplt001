with wks_vietnam_base_detail as (
    select * from {{ ref('vnmwks_integration__wks_vietnam_base_detail') }}
),
vietnam_propagate_from_to as (
    select * from {{ ref('vnmwks_integration__vietnam_propagate_from_to') }}
),
wks_vietnam_siso_propagate_to_details as (
    select * from {{ ref('vnmwks_integration__wks_vietnam_siso_propagate_to_details') }}
),
wks_vietnam_siso_propagate_to_existing_dtls as (
    select * from {{ ref('vnmwks_integration__wks_vietnam_siso_propagate_to_existing_dtls') }}
),
base as (
    SELECT sap_parent_customer_key,
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
    'N' AS propagate_flag,
    CAST(NULL AS INTEGER) propagate_from,
    CAST(NULL AS VARCHAR(100)) AS reason,
    replicated_flag,
    CAST(NULL AS NUMERIC(38, 5)) existing_so_qty,
    CAST(NULL AS NUMERIC(38, 5)) existing_so_value,
    CAST(NULL AS NUMERIC(38, 5)) existing_inv_qty,
    CAST(NULL AS NUMERIC(38, 5)) existing_inv_value,
    CAST(NULL AS NUMERIC(38, 5)) existing_sell_in_qty,
    CAST(NULL AS NUMERIC(38, 5)) existing_sell_in_value,
    CAST(NULL AS NUMERIC(38, 5)) existing_last_3months_so,
    CAST(NULL AS NUMERIC(38, 5)) existing_last_6months_so,
    CAST(NULL AS NUMERIC(38, 5)) existing_last_12months_so,
    CAST(NULL AS NUMERIC(38, 5)) existing_last_3months_so_value,
    CAST(NULL AS NUMERIC(38, 5)) existing_last_6months_so_value,
    CAST(NULL AS NUMERIC(38, 5)) existing_last_12months_so_value
FROM wks_vietnam_base_detail
WHERE (sap_parent_customer_key, month) NOT IN (
        SELECT sap_parent_customer_key,
            propagate_to
        FROM vietnam_propagate_from_to p_from_to
    )
),
propogate1 as (
    SELECT propagated.sap_parent_customer_key,
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
    CAST(propagated.propagate_from AS INTEGER),
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
FROM wks_vietnam_siso_propagate_to_details propagated,
    wks_vietnam_siso_propagate_to_existing_dtls existing
WHERE existing.sap_parent_customer_key = propagated.sap_parent_customer_key
    AND existing.matl_num = propagated.matl_num
    AND existing.month = propagated.month
),
propogate2 as (
    SELECT propagated.sap_parent_customer_key,
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
    CAST(propagated.propagate_from AS INTEGER),
    propagated.reason,
    replicated_flag,
    CAST(NULL AS NUMERIC(38, 5)) so_qty,
    CAST(NULL AS NUMERIC(38, 5)) so_value,
    CAST(NULL AS NUMERIC(38, 5)) inv_qty,
    CAST(NULL AS NUMERIC(38, 5)) inv_value,
    CAST(NULL AS NUMERIC(38, 5)) sell_in_qty,
    CAST(NULL AS NUMERIC(38, 5)) sell_in_value,
    CAST(NULL AS NUMERIC(38, 5)) last_3months_so,
    CAST(NULL AS NUMERIC(38, 5)) last_6months_so,
    CAST(NULL AS NUMERIC(38, 5)) last_12months_so,
    CAST(NULL AS NUMERIC(38, 5)) last_3months_so_value,
    CAST(NULL AS NUMERIC(38, 5)) last_6months_so_value,
    CAST(NULL AS NUMERIC(38, 5)) last_12months_so_value
FROM wks_vietnam_siso_propagate_to_details propagated
WHERE NOT EXISTS (
        SELECT 1
        FROM wks_vietnam_siso_propagate_to_existing_dtls existing
        WHERE existing.sap_parent_customer_key = propagated.sap_parent_customer_key
            AND existing.matl_num = propagated.matl_num
            AND existing.month = propagated.month
    )
),
final as (
    select * from base
    union all
    select * from propogate1
    union all
    select * from propogate2
)
select * from final