WITH affiliate_cancel_wk1
AS (
  SELECT *
  FROM {{ref('jpndcledw_integration__affiliate_cancel_wk1')}}
  ),
tbecorder
AS (
  SELECT *
  FROM {{ref('jpndclitg_integration__tbecorder')}}
  ),
transformed
AS (
  SELECT in1.unique_id --ユニークid
    ,
    in1.orderdate --ns1:order-date
    ,
    in1.webid --ns1:customer-no
    ,
    in1.price --販売額
    ,
    in1.diusrid --顧客no
    ,
    ord.diorderid --ci-next orderid
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
    trunc((ord.ditotalprc - ord.c_didiscountall - ord.diusepoint) / 1.1) - price AS price_sabun --差分金額
  FROM affiliate_cancel_wk1 in1
  INNER JOIN tbecorder ord ON in1.diusrid::NUMERIC = ord.diecusrid::NUMERIC
    -- on in1.diusrid = ord.diecusrid
    AND substring(in1.orderdate, 1, 16) = to_char(ord.dsorderdt, 'YYYY/MM/DD HH24:MI')
  WHERE 1 = 1
    AND ord.dicancel = '0'
    AND ord.dielimflg = '0'
    AND ord.diseikyuprc >= 1 --請求金額1円以上
    AND in1.orderdate != '#N/A'
  ),
final
AS (
  SELECT unique_id::VARCHAR(12) AS unique_id,
    orderdate::VARCHAR(19) AS orderdate,
    webid::VARCHAR(8) AS webid,
    price::NUMBER(38, 0) AS price,
    diusrid::NUMBER(38, 0) AS diusrid,
    diorderid::NUMBER(38, 0) AS diorderid,
    ditotalprc::NUMBER(38, 0) AS ditotalprc,
    diseikyuprc::NUMBER(38, 0) AS diseikyuprc,
    diordertax::NUMBER(38, 0) AS diordertax,
    c_didiscountprc::NUMBER(38, 0) AS c_didiscountprc,
    c_didiscountall::NUMBER(38, 0) AS c_didiscountall,
    diusepoint::NUMBER(38, 0) AS diusepoint,
    dihaisoprc::NUMBER(38, 0) AS dihaisoprc,
    c_dicollectprc::NUMBER(38, 0) AS c_dicollectprc,
    c_ditoujitsuhaisoprc::NUMBER(38, 0) AS c_ditoujitsuhaisoprc,
    price_zeinuki::NUMBER(38, 0) AS price_zeinuki,
    judge_price::NUMBER(20, 0) AS judge_price,
    price_sabun::NUMBER(38, 0) AS price_sabun
  FROM transformed
  )
SELECT *
FROM final