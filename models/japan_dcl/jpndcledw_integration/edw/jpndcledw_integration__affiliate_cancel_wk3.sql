WITH affiliate_cancel_wk1
AS (
  SELECT *
  FROM {{ref('jpndcledw_integration__affiliate_cancel_wk1')}}
  ),
affiliate_cancel_wk2
AS (
  SELECT *
  FROM {{ref('jpndcledw_integration__affiliate_cancel_wk2')}}
  ),
transformed
AS (
  SELECT i1.unique_id,
    i1.orderdate,
    i1.price,
    i2.price_zeinuki,
    i2.ditotalprc,
    i2.diordertax,
    i2.c_didiscountprc,
    i2.c_didiscountall,
    i2.diusepoint,
    i2.dihaisoprc,
    i2.c_dicollectprc,
    i2.c_ditoujitsuhaisoprc,
    i2.diseikyuprc,
    i2.diorderid,
    lpad(i1.diusrid, 10, '0') AS diusrid,
    lpad(i1.webid, 8, '0') AS webid,
    i2.judge_price,
    i2.price_sabun,
    CASE 
      WHEN i2.judge_price IS NULL
        THEN 'データなし'
      ELSE CASE 
          WHEN i2.price_sabun BETWEEN - 100
              AND 100
            THEN '一致'
          ELSE '金額不一致'
          END
      END AS cmpr_sts,
    '*' AS samday_odr_flg
  FROM affiliate_cancel_wk1 i1
  LEFT JOIN affiliate_cancel_wk2 i2 ON i1.unique_id = i2.unique_id
  ),
final
AS (
  SELECT unique_id::VARCHAR(12) AS rcv_orderid,
    orderdate::VARCHAR(19) AS rcv_orderdt,
    price::number(38, 0) AS rcv_price,
    price_zeinuki::number(38, 0) AS cnxt_price_tax_excl,
    ditotalprc::number(38, 0) AS cnxt_ditotalprc,
    diordertax::number(38, 0) AS cnxt_diordertax,
    c_didiscountprc::number(38, 0) AS cnxt_c_didiscountprc,
    c_didiscountall::number(38, 0) AS cnxt_c_didiscountall,
    diusepoint::number(38, 0) AS cnxt_diusepoint,
    dihaisoprc::number(38, 0) AS cnxt_dihaisoprc,
    c_dicollectprc::number(38, 0) AS cnxt_c_dicollectprc,
    c_ditoujitsuhaisoprc::number(38, 0) AS cnxt_c_ditoujitsuhaisoprc,
    diseikyuprc::number(38, 0) AS cnxt_diseikyuprc,
    diorderid::number(38, 0) AS cnxt_diorderid,
    diusrid::VARCHAR(40) AS diusrid,
    webid::VARCHAR(32) AS webid,
    judge_price::number(20, 0) AS judge_price,
    price_sabun::number(38, 0) AS price_sabun,
    cmpr_sts::VARCHAR(15) AS cmpr_sts,
    samday_odr_flg::VARCHAR(1) AS samday_odr_flg
  FROM transformed
  )
SELECT *
FROM final