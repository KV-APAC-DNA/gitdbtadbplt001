WITH affiliate_cancel_wk3
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.affiliate_cancel_wk3
  ),
tbecorder
AS (
  SELECT *
  FROM dev_dna_core.snapjpdclitg_integration.tbecorder
  ),
transformed
AS (
  SELECT in1.diusrid,
    in1.webid,
    in1.rcv_orderid,
    in1.rcv_orderdt,
    in1.rcv_price,
    ord.diorderid,
    ord.dsorderdt --受注日
    ,
    ord.ditotalprc --受注金額（税込）
    ,
    ord.diseikyuprc --請求金額（税込）
    ,
    ord.diordertax --消費税額
    ,
    ord.c_didiscountprc --割引金額
    ,
    ord.c_didiscountall --値引金額
    ,
    ord.diusepoint --使用ポイント数
    ,
    ord.dihaisoprc --配送料金
    ,
    ord.c_dicollectprc --代引き手数料
    ,
    ord.c_ditoujitsuhaisoprc --当日配送手数料
    ,
    ord.ditotalprc - ord.diordertax AS price_zeinuki --税抜き金額
    ,
    trunc((ord.ditotalprc - ord.c_didiscountall - ord.diusepoint) / 1.1) AS judge_price --比較用金額
    ,
    trunc((ord.ditotalprc - ord.c_didiscountall - ord.diusepoint) / 1.1) - rcv_price AS price_sabun --差分金額
  FROM affiliate_cancel_wk3 in1
  INNER JOIN tbecorder ord ON in1.diusrid::NUMERIC = ord.diecusrid::NUMERIC
    AND substring(in1.rcv_orderdt, 1, 10) = to_char(ord.dsorderdt, 'yyyy/mm/dd')
  WHERE 1 = 1
    AND ord.dicancel = '0'
    AND ord.dielimflg = '0'
    AND ord.diseikyuprc >= 1 --請求金額1円以上
    AND in1.cmpr_sts = 'データなし'
  ),
final
AS (
  SELECT diusrid::VARCHAR(40) AS diusrid,
    webid::VARCHAR(32) AS webid,
    rcv_orderid::VARCHAR(12) AS rcv_orderid,
    rcv_orderdt::VARCHAR(19) AS rcv_orderdt,
    rcv_price::number(18, 0) AS rcv_price,
    diorderid::number(18, 0) AS diorderid,
    dsorderdt::timestamp_ntz(9) AS dsorderdt,
    ditotalprc::number(18, 0) AS ditotalprc,
    diseikyuprc::number(18, 0) AS diseikyuprc,
    diordertax::number(18, 0) AS diordertax,
    c_didiscountprc::number(18, 0) AS c_didiscountprc,
    c_didiscountall::number(18, 0) AS c_didiscountall,
    diusepoint::number(18, 0) AS diusepoint,
    dihaisoprc::number(18, 0) AS dihaisoprc,
    c_dicollectprc::number(18, 0) AS c_dicollectprc,
    c_ditoujitsuhaisoprc::number(18, 0) AS c_ditoujitsuhaisoprc,
    price_zeinuki::number(18, 0) AS price_zeinuki,
    judge_price::number(18, 0) AS judge_price,
    price_sabun::number(18, 0) AS price_sabun,
    current_timestamp()::timestamp_ntz(9) AS inserted_date,
    current_timestamp()::timestamp_ntz(9) AS updated_date
  FROM transformed
  )
SELECT *
FROM final