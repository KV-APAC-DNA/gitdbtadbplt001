WITH tt02salem_sum_mv_mt_tbl
AS (
  SELECT *
  FROM {{ source('jpdcledw_integration', 'tt02salem_sum_mv_mt_tbl') }}
  ),
final
AS (
  SELECT tt02salem_sum_mv_mt_tbl.saleno,
    tt02salem_sum_mv_mt_tbl.gyono,
    tt02salem_sum_mv_mt_tbl.meisaikbn,
    tt02salem_sum_mv_mt_tbl.itemcode,
    tt02salem_sum_mv_mt_tbl.wariritu,
    tt02salem_sum_mv_mt_tbl.tanka,
    tt02salem_sum_mv_mt_tbl.warimaekomitanka,
    tt02salem_sum_mv_mt_tbl.suryo,
    tt02salem_sum_mv_mt_tbl.kingaku,
    tt02salem_sum_mv_mt_tbl.warimaekomikingaku,
    tt02salem_sum_mv_mt_tbl.meisainukikingaku,
    tt02salem_sum_mv_mt_tbl.warimaenukikingaku,
    tt02salem_sum_mv_mt_tbl.kesaiid,
    tt02salem_sum_mv_mt_tbl.saleno_trm,
    tt02salem_sum_mv_mt_tbl.maker,
    tt02salem_sum_mv_mt_tbl.salemrowid
  FROM tt02salem_sum_mv_mt_tbl
  )
SELECT *
FROM final
