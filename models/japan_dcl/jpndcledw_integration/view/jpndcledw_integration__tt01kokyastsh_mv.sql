WITH tt01kokyastsh_mv_tbl
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tt01kokyastsh_mv_tbl') }}
  ),
final
AS (
  SELECT tt01kokyastsh_mv_tbl.saleno,
    tt01kokyastsh_mv_tbl.juchkbn,
    tt01kokyastsh_mv_tbl.juchdate,
    tt01kokyastsh_mv_tbl.kokyano,
    tt01kokyastsh_mv_tbl.torikeikbn,
    tt01kokyastsh_mv_tbl.cancelflg,
    tt01kokyastsh_mv_tbl.hanrocode,
    tt01kokyastsh_mv_tbl.syohanrobunname,
    tt01kokyastsh_mv_tbl.chuhanrobunname,
    tt01kokyastsh_mv_tbl.daihanrobunname,
    tt01kokyastsh_mv_tbl.sogokei,
    tt01kokyastsh_mv_tbl.tenpocode,
    tt01kokyastsh_mv_tbl.saleno_trm,
    tt01kokyastsh_mv_tbl.maker,
    tt01kokyastsh_mv_tbl.salehrowid
  FROM tt01kokyastsh_mv_tbl
  )
SELECT *
FROM final