WITH tt02salem_hen_sub_mv_mt
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tt02salem_hen_sub_mv_mt
  ),
transformed
AS (
  SELECT
        SALENO
        ,GYONO
        ,MEISAIKBN
        ,ITEMCODE
        ,SUM(SURYO)	AS SURYO
        ,TANKA
        ,SUM(KINGAKU)	AS KINGAKU
        ,SUM(MEISAINUKIKINGAKU) AS MEISAINUKIKINGAKU
        ,WARIRITU
        ,SUM(WARIMAEKOMITANKA) AS WARIMAEKOMITANKA
        ,SUM(WARIMAENUKIKINGAKU) AS WARIMAENUKIKINGAKU
        ,SUM(WARIMAEKOMIKINGAKU) AS WARIMAEKOMIKINGAKU
        ,DISPSALENO
        ,KESAIID
        ,HENPINSTS
        ,COUNT(*) cnt
  FROM tt02salem_hen_sub_mv_mt
  GROUP BY
        SALENO
        ,GYONO
        ,MEISAIKBN
        ,ITEMCODE
        ,TANKA
        ,WARIRITU
        ,DISPSALENO
        ,KESAIID
        ,HENPINSTS 
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(6) as meisaikbn,
        itemcode::varchar(45) as itemcode,
        suryo::number(18,0) as suryo,
        tanka::number(18,0) as tanka,
        kingaku::number(18,0) as kingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        wariritu::number(18,0) as wariritu,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        dispsaleno::varchar(60) as dispsaleno,
        kesaiid::number(10,0) as kesaiid,
        henpinsts::varchar(6) as henpinsts,
        cnt::number(18,0) as cnt
  FROM transformed
)
SELECT *
FROM final