WITH tt01saleh_uri_mv_tb1
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tt01saleh_uri_mv_tbl') }}
  ),
final
AS (
  SELECT tt01saleh_uri_mv_tb1.saleno,
    tt01saleh_uri_mv_tb1.juchkbn,
    tt01saleh_uri_mv_tb1.juchdate,
    tt01saleh_uri_mv_tb1.kokyano,
    tt01saleh_uri_mv_tb1.hanrocode,
    tt01saleh_uri_mv_tb1.syohanrobunname,
    tt01saleh_uri_mv_tb1.chuhanrobunname,
    tt01saleh_uri_mv_tb1.daihanrobunname,
    tt01saleh_uri_mv_tb1.mediacode,
    tt01saleh_uri_mv_tb1.kessaikbn,
    tt01saleh_uri_mv_tb1.soryo,
    tt01saleh_uri_mv_tb1.tax,
    tt01saleh_uri_mv_tb1.sogokei,
    tt01saleh_uri_mv_tb1.cardcorpcode,
    tt01saleh_uri_mv_tb1.henreasoncode,
    tt01saleh_uri_mv_tb1.cancelflg,
    tt01saleh_uri_mv_tb1.insertdate,
    tt01saleh_uri_mv_tb1.inserttime,
    tt01saleh_uri_mv_tb1.insertid,
    tt01saleh_uri_mv_tb1.updatedate,
    tt01saleh_uri_mv_tb1.updatetime,
    tt01saleh_uri_mv_tb1.zipcode,
    tt01saleh_uri_mv_tb1.todofukencode,
    tt01saleh_uri_mv_tb1.happenpoint,
    tt01saleh_uri_mv_tb1.riyopoint,
    tt01saleh_uri_mv_tb1.shukkasts,
    tt01saleh_uri_mv_tb1.torikeikbn,
    tt01saleh_uri_mv_tb1.tenpocode,
    tt01saleh_uri_mv_tb1.shukadate,
    tt01saleh_uri_mv_tb1.rank,
    tt01saleh_uri_mv_tb1.dispsaleno,
    tt01saleh_uri_mv_tb1.kesaiid,
    tt01saleh_uri_mv_tb1.henreasonname,
    tt01saleh_uri_mv_tb1.uketsukeusrid,
    tt01saleh_uri_mv_tb1.uketsuketelcompanycd,
    tt01saleh_uri_mv_tb1.smkeiroid,
    tt01saleh_uri_mv_tb1.dipromid,
    tt01saleh_uri_mv_tb1.kesairowid,
    tt01saleh_uri_mv_tb1.orderrowid,
    tt01saleh_uri_mv_tb1.usercardrowid,
    tt01saleh_uri_mv_tb1.promorowid,
    tt01saleh_uri_mv_tb1.hanyorowid
  FROM tt01saleh_uri_mv_tb1
  )
SELECT *
FROM final
