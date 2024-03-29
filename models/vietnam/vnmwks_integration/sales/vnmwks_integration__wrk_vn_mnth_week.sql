with edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
weeks AS (
  SELECT DISTINCT
    t."year",
    t.qrtr_no,
    t.qrtr,
    t.mnth_id,
    t.mnth_desc,
    t.mnth_no,
    t.mnth_shrt,
    t.mnth_long,
    t.wk,
    t.mnth_wk_no,
    MIN(t.cal_date_id) AS frst_day,
    MAX(t.cal_date_id) AS lst_day,
    COUNT(mnth_wk_no) OVER (PARTITION BY mnth_id) AS cnt
  FROM edw_vw_os_time_dim AS t
  GROUP BY
    t."year",
    t.qrtr_no,
    t.qrtr,
    t.mnth_id,
    t.mnth_desc,
    t.mnth_no,
    t.mnth_shrt,
    t.mnth_long,
    t.wk,
    t.mnth_wk_no
  ORDER BY
    mnth_id,
    mnth_wk_no
),
transformed as(
    SELECT
  prim_month.*,
  p.p_1,
  p.p_2
FROM weeks AS prim_month, (
  SELECT
    *
  FROM (
    SELECT
      w.*,
      LAG(mnth_id, 1) OVER (ORDER BY rn DESC) AS p_1,
      LAG(mnth_id, 2) OVER (ORDER BY rn DESC) AS p_2
    FROM (
      SELECT DISTINCT
        mnth_id,
        ROW_NUMBER() OVER (ORDER BY mnth_id DESC) AS rn
      FROM (
        SELECT DISTINCT
          mnth_id
        FROM weeks
        WHERE
          cnt = 5
      )
      ORDER BY
        rn DESC
    ) AS w
    UNION ALL
    SELECT
      w.*,
      LAG(mnth_id, 1) OVER (ORDER BY rn DESC) AS p_1,
      LAG(mnth_id, 2) OVER (ORDER BY rn DESC) AS p_2
    FROM (
      SELECT DISTINCT
        mnth_id,
        ROW_NUMBER() OVER (ORDER BY mnth_id DESC) AS rn
      FROM (
        SELECT DISTINCT
          mnth_id
        FROM weeks
        WHERE
          cnt = 4
      )
      ORDER BY
        rn DESC
    ) AS w
  )
) AS p
WHERE
  prim_month.mnth_id = p.MNTH_ID(+)
ORDER BY
  mnth_id,
  wk
)
select * from transformed