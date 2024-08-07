with 
kr_054_sfuyo_meisai as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_054_SFUYO_MEISAI
),
kr_054_cal_v as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.KR_054_CAL_V
),
final as (
 SELECT
       '01_付与P' as COL1,
       V.YYMM as POINT_YM,
       SUBSTRING(V.YYMM,1,4) as POINT_YY,
       SUBSTRING(V.YYMM,5,2) as POINT_MM,
       SUM(NVL(MEI.POINT,0)) as POINT_SUM
 FROM
       KR_054_CAL_V V
 LEFT JOIN 
   KR_054_SFUYO_MEISAI MEI
  ON
       V.YYMM=MEI.POINT_YM
 GROUP BY
       COL1,
       V.YYMM,
       SUBSTRING(V.YYMM,1,4),
       SUBSTRING(V.YYMM,5,2)
)
select * from final