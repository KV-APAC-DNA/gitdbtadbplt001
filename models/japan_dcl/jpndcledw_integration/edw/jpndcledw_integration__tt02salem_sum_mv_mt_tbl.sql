WITH tt02salem_uri_mv_mt
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tt02salem_uri_mv_mt
  ),
tt02salem_hen_mv_mt
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tt02salem_hen_mv_mt
  ),
transformed
AS (
  SELECT
        SALENO			AS	SALENO
        ,GYONO			AS	GYONO
        ,MEISAIKBN		AS	MEISAIKBN
        ,ITEMCODE		AS	ITEMCODE
        ,WARIRITU		AS	WARIRITU
        ,TANKA			AS	TANKA
        ,WARIMAEKOMITANKA	AS	WARIMAEKOMITANKA
        ,SURYO			AS	SURYO
        ,KINGAKU		AS	KINGAKU
        ,WARIMAEKOMIKINGAKU	AS	WARIMAEKOMIKINGAKU
        ,MEISAINUKIKINGAKU	AS	MEISAINUKIKINGAKU
        ,WARIMAENUKIKINGAKU	AS	WARIMAENUKIKINGAKU
        ,KESAIID		AS	KESAIID
        ,trim(SALENO)	AS	SALENO_TRM
        ,1 as maker
        ,NULL AS SALEMROWID
  FROM tt02salem_uri_mv_mt
  UNION ALL
  SELECT
        SALENO			AS	SALENO
        ,GYONO			AS	GYONO
        ,MEISAIKBN		AS	MEISAIKBN
        ,ITEMCODE		AS	ITEMCODE
        ,WARIRITU		AS	WARIRITU
        ,TANKA			AS	TANKA
        ,WARIMAEKOMITANKA	AS	WARIMAEKOMITANKA
        ,SURYO			AS	SURYO
        ,KINGAKU		AS	KINGAKU
        ,WARIMAEKOMIKINGAKU	AS	WARIMAEKOMIKINGAKU
        ,MEISAINUKIKINGAKU	AS	MEISAINUKIKINGAKU
        ,WARIMAENUKIKINGAKU	AS	WARIMAENUKIKINGAKU
        ,KESAIID		AS	KESAIID
        ,trim(SALENO)	AS	SALENO_TRM
        ,2 as maker
        ,NULL as SALEMROWID
  FROM tt02salem_hen_mv_mt 
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(6) as meisaikbn,
        itemcode::varchar(45) as itemcode,
        wariritu::number(18,0) as wariritu,
        tanka::number(18,0) as tanka,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        suryo::number(18,0) as suryo,
        kingaku::number(18,0) as kingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        kesaiid::number(18,0) as kesaiid,
        saleno_trm::varchar(62) as saleno_trm,
        maker::number(18,0) as maker,
        salemrowid::varchar(1) as salemrowid
  FROM transformed
)
SELECT *
FROM final