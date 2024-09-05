with india_propagate_to as
(
  select * from ({{ ref('indwks_integration__india_propagate_to') }})
),
wks_india_base_detail as 
(
    select * from ({{ ref('indwks_integration__wks_india_base_detail') }})
),
trans as
(
    SELECT p_to.sap_parent_customer_key,
  p_to.latest_month,
  p_to.month propagate_to,
  base.month propagate_from,
  base.so_qty,
  base.inv_qty,
  DATEDIFF(month, TO_DATE(CAST(base.month AS STRING), 'YYYYMM'), TO_DATE(CAST(latest_month AS STRING), 'YYYYMM')) AS diff_month,
  --datediff(month, to_date(base.month, 'YYYYMM'), to_date(latest_month, 'YYYYMM')) diff_month,
  p_to.reason
FROM (
  SELECT *,
    CASE 
      WHEN propagate_flag = 'Y'
        AND (
          nvl(so_value, 0) = 0
          AND nvl(inv_value, 0) = 0
          )
        THEN 'Sellout and Inventory Missing'
      WHEN propagate_flag = 'Y'
        AND nvl(so_value, 0) = 0
        THEN 'Sellout Missing'
      WHEN propagate_flag = 'Y'
        AND nvl(inv_value, 0) = 0
        THEN 'Inventory Missing'
      ELSE 'Not Propagate'
      END AS reason
  FROM india_propagate_to
  WHERE propagate_flag = 'Y'
  ) p_to,
  (
    SELECT d.sap_parent_customer_key, --d.sap_parent_customer_desc,
      d.month AS month,
      sum(so_qty) so_qty,
      sum(so_value) so_value,
      sum(inv_qty) inv_qty,
      sum(inv_value) inv_value
    FROM (
      SELECT sap_parent_customer_key, --sap_parent_customer_desc, 
        max(month) AS month
      FROM (
        SELECT sap_parent_customer_key, --sap_parent_customer_desc,
          month
        FROM wks_india_base_detail
        WHERE (
            sap_parent_customer_key,
            month
            ) NOT IN (
            SELECT sap_parent_customer_key,
              month
            FROM INDIA_propagate_to
            WHERE propagate_flag = 'Y'
            )
        GROUP BY sap_parent_customer_key,
          month
        HAVING (
            sum(so_qty) > 0
            AND sum(inv_qty) > 0
            )
        ) all_months
      GROUP BY sap_parent_customer_key --,sap_parent_customer_desc
      ) max_month,
      wks_india_base_detail d
    WHERE max_month.sap_parent_customer_key = d.sap_parent_customer_key
      AND max_month.month = d.month
    GROUP BY d.sap_parent_customer_key, --d.sap_parent_customer_desc,
      d.month
    ) base
WHERE p_to.sap_parent_customer_key = base.sap_parent_customer_key
  AND base.month < p_to.month
  AND datediff(month, to_date(base.month, 'YYYYMM'), to_date(latest_month, 'YYYYMM')) <= 2
)
select * from trans