WITH affiliate_cancel_receive
AS (
  SELECT *
  FROM {{ref('jpndclitg_integration__affiliate_cancel_receive')}}
  ),
tbusrpram
AS (
  SELECT *
  FROM {{ref('jpndclitg_integration__tbusrpram')}}
  ),
ct1
AS (
  SELECT rcv.unique_id,
    rcv.orderdate,
    rcv.webid,
    sum(rcv.amount_excluded_tax) AS price,
    usr.diusrid
  FROM affiliate_cancel_receive rcv
  LEFT JOIN tbusrpram usr ON rcv.webid::INTEGER = usr.dsctrlcd::INTEGER
    AND usr.dielimflg = '0' --削除フラグ
    AND usr.disecessionflg = '0' --退会除外
  WHERE rcv.STATUS IS NULL
    AND webid != '#N/A'
  GROUP BY rcv.unique_id,
    rcv.orderdate,
    rcv.webid,
    usr.diusrid
  ),
ct2
AS (
  SELECT rcv.unique_id,
    rcv.orderdate,
    rcv.webid,
    sum(rcv.amount_excluded_tax) AS price,
    usr.diusrid
  FROM affiliate_cancel_receive rcv
  LEFT JOIN tbusrpram usr ON rcv.webid = usr.dsctrlcd
    AND usr.dielimflg = '0' --削除フラグ
    AND usr.disecessionflg = '0' --退会除外
  WHERE rcv.STATUS IS NULL
    AND webid = '#N/A'
  GROUP BY rcv.unique_id,
    rcv.orderdate,
    rcv.webid,
    usr.diusrid
  ORDER BY 1
  ),
trns
AS (
  SELECT *
  FROM ct1
  
  UNION ALL
  
  SELECT *
  FROM ct2
  ),
final
AS (
  SELECT unique_id::VARCHAR(12) AS unique_id,
    orderdate::VARCHAR(19) AS orderdate,
    webid::VARCHAR(8) AS webid,
    price::number(38, 0) AS price,
    diusrid::number(38, 0) AS diusrid
  FROM trns
  )
SELECT *
FROM final