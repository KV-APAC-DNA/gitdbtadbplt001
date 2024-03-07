with WKS_TH_watsons_base_detail as(
    select * from  DEV_DNA_CORE.ASING012_WORKSPACE.THAWKS_INTEGRATION__WKS_TH_WATSONS_BASE_DETAIL
),
edw_vw_os_time_dim as(
    select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.EDW_VW_OS_TIME_DIM
),
trans as(
      SELECT
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month,
    SUM(so_qty) AS so_qty,
    SUM(so_value) AS so_value,
    SUM(inv_qty) AS inv_qty,
    SUM(inv_value) AS inv_value
  FROM WKS_TH_watsons_base_detail
  WHERE
    month /* replicated_flag = 'N' */ <= (
      SELECT DISTINCT
        cal_mnth_id
      FROM EDW_VW_OS_TIME_DIM
      WHERE
        cal_date = CURRENT_DATE
    )
  GROUP BY
    sap_parent_customer_key,
    sap_parent_customer_desc,
    month
),
trans2 as(
    SELECT DISTINCT
            cal_YEAR AS year,
            cal_mnth_id AS mnth_id
          FROM EDW_VW_OS_TIME_DIM
          WHERE
            cal_mnth_id <= (
              SELECT DISTINCT
                cal_mnth_id
              FROM EDW_VW_OS_TIME_DIM
              WHERE
                cal_date = CURRENT_DATE
            )
),
transformed as(
SELECT
  sap_parent_customer_key,
  sap_parent_customer_desc,
  month,
  so_qty,
  so_value,
  inv_qty,
  inv_value,
  CASE
    WHEN month > (
      /* mnth_id, */
      SELECT
        third_month
      FROM (
        SELECT
          mnth_id,
          LAG(mnth_id, 2) OVER (ORDER BY mnth_id) AS third_month
        FROM trans2
      )
      WHERE
        mnth_id = (
          SELECT DISTINCT
            cal_mnth_id
          FROM EDW_VW_OS_TIME_DIM
          WHERE
            cal_date = CURRENT_DATE
        )
    )
    AND (
      COALESCE(so_value, 0) = 0 OR COALESCE(inv_value, 0) = 0
    )
    THEN 'Y'
    ELSE 'N'
  END AS propagate_flag,
  MAX(month) OVER (PARTITION BY sap_parent_customer_key, sap_parent_customer_desc) AS latest_month
FROM trans
)
select * from transformed