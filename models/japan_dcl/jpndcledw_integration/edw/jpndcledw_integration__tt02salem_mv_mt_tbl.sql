WITH tt02salem_sum_mv_mt
AS (
  SELECT *
  FROM  {{ ref('jpndcledw_integration__tt02salem_sum_mv_mt') }}
  ),
tt02salem_add_uri_mv_mt
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tt02salem_add_uri_mv_mt') }}
  ),
tt02salem_add_hen_mv_mt
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tt02salem_add_hen_mv_mt') }}
  ),
transformed
AS (
  SELECT
        SALENO                    AS     SALENO
        ,GYONO              AS     GYONO
        ,MEISAIKBN          AS     MEISAIKBN
        ,ITEMCODE           AS     ITEMCODE
        ,WARIRITU           AS     WARIRITU
        ,TANKA              AS     TANKA
        ,WARIMAEKOMITANKA   AS     WARIMAEKOMITANKA
        ,SURYO              AS     SURYO
        ,KINGAKU            AS     KINGAKU
        ,WARIMAEKOMIKINGAKU AS     WARIMAEKOMIKINGAKU
        ,MEISAINUKIKINGAKU  AS     MEISAINUKIKINGAKU
        ,WARIMAENUKIKINGAKU AS     WARIMAENUKIKINGAKU
        ,KESAIID            AS     KESAIID
        ,trim(SALENO) AS     SALENO_TRM
    FROM tt02salem_sum_mv_mt

    union all

    SELECT tt02salem_add_uri_mv_mt.saleno, tt02salem_add_uri_mv_mt.gyono, tt02salem_add_uri_mv_mt.meisaikbn, 
        tt02salem_add_uri_mv_mt.itemcode, tt02salem_add_uri_mv_mt.wariritu, 
        tt02salem_add_uri_mv_mt.tanka, tt02salem_add_uri_mv_mt.warimaekomitanka, 
        tt02salem_add_uri_mv_mt.suryo, tt02salem_add_uri_mv_mt.kingaku, 
        tt02salem_add_uri_mv_mt.warimaekomikingaku, tt02salem_add_uri_mv_mt.meisainukikingaku, 
        tt02salem_add_uri_mv_mt.warimaenukikingaku, tt02salem_add_uri_mv_mt.kesaiid,
        trim(tt02salem_add_uri_mv_mt.saleno::text)::character varying AS saleno_trm
    FROM tt02salem_add_uri_mv_mt

    union all

    SELECT tt02salem_add_hen_mv_mt.saleno, 
        tt02salem_add_hen_mv_mt.gyono, 
        tt02salem_add_hen_mv_mt.meisaikbn, 
        tt02salem_add_hen_mv_mt.itemcode, tt02salem_add_hen_mv_mt.wariritu, 
        tt02salem_add_hen_mv_mt.tanka, tt02salem_add_hen_mv_mt.warimaekomitanka,
        tt02salem_add_hen_mv_mt.suryo, tt02salem_add_hen_mv_mt.kingaku, 
        tt02salem_add_hen_mv_mt.warimaekomikingaku, tt02salem_add_hen_mv_mt.meisainukikingaku, 
        tt02salem_add_hen_mv_mt.warimaenukikingaku, tt02salem_add_hen_mv_mt.kesaiid, 
        trim(tt02salem_add_hen_mv_mt.saleno::text)::character varying AS saleno_trm
    FROM tt02salem_add_hen_mv_mt 
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        gyono::number(18,0) as gyono,
        meisaikbn::varchar(36) as meisaikbn,
        itemcode::varchar(60) as itemcode,
        wariritu::number(18,0) as wariritu,
        tanka::number(18,0) as tanka,
        warimaekomitanka::number(18,0) as warimaekomitanka,
        suryo::number(18,0) as suryo,
        kingaku::number(18,0) as kingaku,
        warimaekomikingaku::number(18,0) as warimaekomikingaku,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        kesaiid::number(18,0) as kesaiid,
        saleno_trm::varchar(62) as saleno_trm
  FROM transformed
)
SELECT *
FROM final