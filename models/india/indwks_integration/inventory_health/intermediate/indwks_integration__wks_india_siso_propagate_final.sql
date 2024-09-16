with wks_india_base_detail as
(
    select * from ({{ ref('indwks_integration__wks_india_base_detail') }})
),
india_propagate_from_to as
(
    select * from ({{ ref('indwks_integration__india_propagate_from_to') }})
),
wks_india_siso_propagate_to_details as
(
    select * from ({{ ref('indwks_integration__wks_india_siso_propagate_to_details') }})
),
wks_india_siso_propagate_to_existing_dtls as
(
    select * from ({{ ref('indwks_integration__wks_india_siso_propagate_to_existing_dtls') }})
),
union_1 as
(
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
          'N' AS propagate_flag,
          cast(NULL AS INTEGER) propagate_from,
          cast(NULL AS VARCHAR(100)) AS reason,
          replicated_flag,
          cast(NULL AS NUMERIC(38, 5)) existing_so_qty,
          cast(NULL AS NUMERIC(38, 5)) existing_so_value,
          cast(NULL AS NUMERIC(38, 5)) existing_inv_qty,
          cast(NULL AS NUMERIC(38, 5)) existing_inv_value,
          cast(NULL AS NUMERIC(38, 5)) existing_sell_in_qty,
          cast(NULL AS NUMERIC(38, 5)) existing_sell_in_value,
          cast(NULL AS NUMERIC(38, 5)) existing_last_3months_so,
          cast(NULL AS NUMERIC(38, 5)) existing_last_6months_so,
          cast(NULL AS NUMERIC(38, 5)) existing_last_12months_so,
          cast(NULL AS NUMERIC(38, 5)) existing_last_3months_so_value,
          cast(NULL AS NUMERIC(38, 5)) existing_last_6months_so_value,
          cast(NULL AS NUMERIC(38, 5)) existing_last_12months_so_value
    FROM wks_india_base_detail
        WHERE (
            sap_parent_customer_key,
        month
        ) NOT IN (
        SELECT sap_parent_customer_key,
          propagate_to
        FROM india_propagate_from_to p_from_to
        )
),
union_2 as
(
    SELECT 
          propagated.sap_parent_customer_key,
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
          cast(propagated.propagate_from AS INTEGER) as propagate_from,
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
    FROM wks_india_siso_propagate_to_details propagated,
          wks_india_siso_propagate_to_existing_dtls existing
        WHERE existing.sap_parent_customer_key = propagated.sap_parent_customer_key
          AND existing.matl_num = propagated.matl_num
          AND existing.month = propagated.month
    ),
union_3 as
(
    SELECT 
        propagated.sap_parent_customer_key,
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
        cast(propagated.propagate_from AS INTEGER) as propagate_from,
        propagated.reason,
        replicated_flag,
        cast(NULL AS NUMERIC(38, 5)) as  existing_so_qty,
        cast(NULL AS NUMERIC(38, 5)) as existing_so_value,
        cast(NULL AS NUMERIC(38, 5))  as existing_inv_qty,
        cast(NULL AS NUMERIC(38, 5))  as existing_inv_value,
        cast(NULL AS NUMERIC(38, 5))  as existing_sell_in_qty,
        cast(NULL AS NUMERIC(38, 5))  as existing_sell_in_value,
        cast(NULL AS NUMERIC(38, 5))  as existing_last_3months_so,
        cast(NULL AS NUMERIC(38, 5))  as existing_last_6months_so,
        cast(NULL AS NUMERIC(38, 5))  as existing_last_12months_so,
        cast(NULL AS NUMERIC(38, 5))  as existing_last_3months_so_value,
        cast(NULL AS NUMERIC(38, 5))  as existing_last_6months_so_value,
        cast(NULL AS NUMERIC(38, 5))  as existing_last_12months_so_value
FROM wks_india_siso_propagate_to_details propagated
WHERE NOT EXISTS (
    SELECT 1
    FROM wks_india_siso_propagate_to_existing_dtls existing
    WHERE existing.sap_parent_customer_key = propagated.sap_parent_customer_key
      AND existing.matl_num = propagated.matl_num
      AND existing.month = propagated.month
)
),
final as
(
    select * from union_1
    union all
    select * from union_2
    union all
    select * from union_3
)
select * from final