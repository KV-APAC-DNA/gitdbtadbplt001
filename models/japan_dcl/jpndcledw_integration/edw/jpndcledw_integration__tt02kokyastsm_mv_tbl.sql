WITH tt02kokyastsm_ikou
AS (
  SELECT *
  FROM {{ source('jpdcledw_integration', 'tt02kokyastsm_ikou') }} 
  ),
tt02salem_mv_mt
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__tt02salem_mv_mt') }}
  ),
transformed
AS (
  SELECT
        TT02.SALENO				AS	SALENO
        ,TT02.GYONO					AS	GYONO
        ,TT02.ITEMCODE				AS	ITEMCODE
        ,TT02.MEISAINUKIKINGAKU		AS	MEISAINUKIKINGAKU
        ,TT02.WARIMAENUKIKINGAKU	AS	WARIMAENUKIKINGAKU
        ,TT02.SALENO				AS	SALENO_TRM
        ,1							AS maker
        ,NULL					AS SALEMROWID
  FROM tt02kokyastsm_ikou TT02
  UNION ALL
  SELECT
        TT02.SALENO				AS	SALENO
        ,TT02.GYONO					AS	GYONO
        ,TT02.ITEMCODE				AS	ITEMCODE
        ,TT02.MEISAINUKIKINGAKU		AS	MEISAINUKIKINGAKU
        ,TT02.WARIMAENUKIKINGAKU	AS	WARIMAENUKIKINGAKU
        ,TT02.SALENO_TRM			AS	SALENO_TRM
        ,2							AS maker
        ,NULL					AS SALEMROWID
        --BGN-MOD 20180507 HIGASHI ***変更管理202 顧客ステータス計算の計算方法の見直し(返品の扱い変更)****
  FROM tt02salem_mv_mt TT02
        --FROM CI_DWH_MAIN.TT02SALEM_MV TT02
        --END-MOD 20180507 HIGASHI ***変更管理202 顧客ステータス計算の計算方法の見直し(返品の扱い変更)****COMMENT ON MATERIALIZED VIEW CI_DWH_MAIN.TT02KOKYASTSM_MV IS 'shot table for shot CI_DWH_MAIN.TT02KOKYASTSM_MV'CREATE INDEX CI_DWH_MAIN.TT02KOKYASTSM_MV_PK ON CI_DWH_MAIN.TT02KOKYASTSM_MV (SALENO ASC, GYONO ASC) 
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        gyono::number(18,0) as gyono,
        itemcode::varchar(60) as itemcode,
        meisainukikingaku::number(18,0) as meisainukikingaku,
        warimaenukikingaku::number(18,0) as warimaenukikingaku,
        saleno_trm::varchar(62) as saleno_trm,
        maker::number(18,0) as maker,
        salemrowid::varchar(1) as salemrowid
  FROM transformed
)
SELECT *
FROM final