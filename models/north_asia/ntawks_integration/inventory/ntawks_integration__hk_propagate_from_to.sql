WITH HK_propagate_to
AS (
  SELECT *
  FROM DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.HK_PROPAGATE_TO
  ),
wks_HK_base_detail
AS (
  SELECT *
  FROM DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_HK_BASE_DETAIL
  ),
transformed
AS (
  SELECT p_to.sap_parent_customer_key,
    p_to.latest_month,
    p_to.month propagate_to,
    base.month propagate_from,
    base.so_qty,
    base.inv_qty,
    datediff(month, to_date(base.month, 'YYYYMM'), to_date(latest_month, 'YYYYMM')) diff_month,
    p_to.reason
  FROM (
    SELECT *,
      CASE 
        WHEN propagate_flag = 'Y'
          AND (
            nvl(so_value, 0) = 0
            OR nvl(inv_value, 0) = 0
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
    FROM HK_propagate_to
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
          FROM wks_HK_base_detail
          WHERE (
              sap_parent_customer_key,
              month
              ) NOT IN (
              SELECT sap_parent_customer_key,
                month
              FROM HK_propagate_to
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
        wks_HK_base_detail d
      WHERE max_month.sap_parent_customer_key = d.sap_parent_customer_key
        AND max_month.month = d.month
      GROUP BY d.sap_parent_customer_key, --d.sap_parent_customer_desc,
        d.month
      ) base
  WHERE p_to.sap_parent_customer_key = base.sap_parent_customer_key
    AND base.month < p_to.month
    AND datediff(month, to_date(base.month, 'YYYYMM'), to_date(latest_month, 'YYYYMM')) <= 2
  ),
final
AS (
	SELECT 
    sap_parent_customer_key::varchar(50) as sap_parent_customer_key,
    latest_month::varchar(23) as latest_month,
    propagate_to::varchar(23) as propagate_to,
    propagate_from::varchar(23) as propagate_from,
    so_qty::number(38,0) as so_qty,
    inv_qty::number(38,5) as inv_qty,
    diff_month::number(38,0) as diff_month,
    reason::varchar(29) as reason
	FROM transformed
	)
SELECT *
FROM final
