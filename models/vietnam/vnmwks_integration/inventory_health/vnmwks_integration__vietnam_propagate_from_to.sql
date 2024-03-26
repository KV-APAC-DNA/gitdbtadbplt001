with vietnam_propagate_to as (

),
wks_vietnam_base_detail as (

),
p_to as (
    SELECT *,
            CASE
                WHEN propagate_flag = 'Y'
                AND (
                    nvl (so_value, 0) = 0
                    OR nvl (inv_value, 0) = 0
                ) THEN 'Sellout and Inventory Missing'
                WHEN propagate_flag = 'Y'
                AND nvl (so_value, 0) = 0 THEN 'Sellout Missing'
                WHEN propagate_flag = 'Y'
                AND nvl (inv_value, 0) = 0 THEN 'Inventory Missing'
                ELSE 'Not Propagate'
            END AS reason
        FROM vietnam_propagate_to
        WHERE propagate_flag = 'Y'
),
base as (
    SELECT d.sap_parent_customer_key,
            d.sap_parent_customer_desc,
            d.month AS MONTH,
            SUM(so_qty) so_qty,
            SUM(so_value) so_value,
            SUM(inv_qty) inv_qty,
            SUM(inv_value) inv_value
        FROM (
                SELECT sap_parent_customer_key,
                    sap_parent_customer_desc,
                    MAX(MONTH) AS MONTH
                FROM (
                        SELECT sap_parent_customer_key,
                            sap_parent_customer_desc,
                            MONTH
                        FROM wks_vietnam_base_detail
                        WHERE (sap_parent_customer_key, MONTH) NOT IN (
                                SELECT sap_parent_customer_key,
                                    MONTH
                                FROM vietnam_propagate_to
                                WHERE propagate_flag = 'Y'
                            )
                        GROUP BY sap_parent_customer_key,
                            sap_parent_customer_desc,
                            MONTH
                        HAVING (
                                SUM(so_qty) > 0
                                AND SUM(inv_qty) > 0
                            )
                    ) all_months
                GROUP BY sap_parent_customer_key,
                    sap_parent_customer_desc
            ) max_month,
            wks_vietnam_base_detail d
        WHERE max_month.sap_parent_customer_key = d.sap_parent_customer_key
            AND max_month.month = d.month
        GROUP BY d.sap_parent_customer_key,
            d.sap_parent_customer_desc,
            d.month
),
final as (
    SELECT p_to.sap_parent_customer_key,
    p_to.sap_parent_customer_desc,
    p_to.latest_month,
    p_to.month propagate_to,
    base.month propagate_from,
    base.so_qty,
    base.inv_qty,
    datediff(
        month,
        TO_DATE(base.month, 'YYYYMM'),
        TO_DATE(latest_month, 'YYYYMM')
    ) diff_month,
    p_to.reason
    FROM p_to, base
    WHERE p_to.sap_parent_customer_key = base.sap_parent_customer_key
    AND base.month < p_to.month
    AND datediff(
        month,
        TO_DATE(base.month, 'YYYYMM'),
        TO_DATE(latest_month, 'YYYYMM')
    ) <= 2
)
select * from final