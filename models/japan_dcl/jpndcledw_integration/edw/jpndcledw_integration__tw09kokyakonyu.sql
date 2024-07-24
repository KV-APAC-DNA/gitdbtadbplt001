WITH tw05kokyarecalc
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tw05kokyarecalc
  ),
tt01kokyastsh_mv
AS (
  SELECT --+ LEADING(TT01)
	SALENO_TRM SALENO
	,JUCHKBN
	,TORIKEIKBN
	,JUCHDATE
	,KOKYANO
	,HANROCODE
	,SYOHANROBUNNAME
	,CHUHANROBUNNAME
	,DAIHANROBUNNAME
	,SOGOKEI
	,TENPOCODE
--	 FROM TT01SALEH_MV
   FROM dev_dna_core.jpdcledw_integration.tt01kokyastsh_mv_tbl
   WHERE KOKYANO <> '0000000011' AND KOKYANO <> '9999999944'  --顧客Noが'00000000''99999999'のいずれでもない
	 --AND HANROCODE <> '39'					--通信経路コードが'39'以外
	 AND CANCELFLG = 0					--キャンセルフラグが0
--	 AND JUCHKBN IN ('10','18','19')			--受注区分が'XX''XX''XX'のいずれか
	 AND JUCHKBN NOT LIKE '9%'
	 AND KOKYANO IN (SELECT KOKYANO FROM tw05kokyarecalc)  --TW05KOKYARECALCの顧客No.
  ),
tm24_item
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.tm24_item
  ),
hanbai_shohin_attr
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.hanbai_shohin_attr
  ),
tt02kokyastsm_mv
AS (
  SELECT --+ USE_HASH(TT02)
	TT02.SALENO_TRM AS SALENO
	,SUM(case when TT02.MEISAINUKIKINGAKU > 0 then TT02.MEISAINUKIKINGAKU else 0 end) AS MEISAINUKIKINGAKU  --SU割引後税抜金額
	,SUM(case when TT02.MEISAINUKIKINGAKU > 0 then TT02.WARIMAENUKIKINGAKU else 0 end) AS WARIMAENUKIKINGAKU  --SU割引前税抜金額
	,SUM(CASE WHEN TM24.BRANDCODE = '60000' and TT02.MEISAINUKIKINGAKU > 0 THEN TT02.MEISAINUKIKINGAKU ELSE 0 END) AS GMEISAINUKIKINGAKU  --SU割引後税抜金額うちジェノマー商品の金額
	,MAX(case nvl(cast(E.BUMON6_ADD_ATTR2 as NUMERIC),1) when 0 then 1 else 0 end) AS JUCHITEMKBNSHOHIN
	,MAX(case nvl(cast(E.BUMON6_ADD_ATTR1 as NUMERIC),1) when 0 then 1 else 0 end) AS JUCHITEMKBNTRIAL
	,MAX(case nvl(CAST(E.BUMON6_ADD_ATTR3 as NUMERIC),1) when 0 then 1 else 0 end) AS JUCHITEMKBNSAMPLE
--	 FROM TT02SALEM_MV TT02
  FROM dev_dna_core.jpdcledw_integration.tt02kokyastsm_mv_tbl TT02
	 LEFT JOIN tm24_item TM24 ON TT02.ITEMCODE = TM24.ITEMCODE
	 LEFT JOIN hanbai_shohin_attr E ON TT02.ITEMCODE = E.SHOHIN_CODE --■追加
  GROUP BY TT02.SALENO_TRM
  ),
v01kokyakonyu
AS (
   SELECT  --+ USE_HASH(T01,T02)
	T01.SALENO
	,T01.JUCHKBN
	,T01.TORIKEIKBN
	,T01.JUCHDATE
	,T01.KOKYANO
	,T01.HANROCODE
	,T01.SYOHANROBUNNAME
	,T01.CHUHANROBUNNAME
	,T01.DAIHANROBUNNAME
	,T01.SOGOKEI
	,T01.TENPOCODE
	,T02.MEISAINUKIKINGAKU
	,T02.WARIMAENUKIKINGAKU
	,T02.GMEISAINUKIKINGAKU
	,T02.JUCHITEMKBNSHOHIN
	,T02.JUCHITEMKBNTRIAL
	,T02.JUCHITEMKBNSAMPLE
	FROM
	(
	 SELECT *
     FROM tt01kokyastsh_mv
	) T01
	 INNER JOIN
	(
	SELECT *
    FROM tt02kokyastsm_mv
	) T02 ON T01.SALENO = T02.SALENO
 ),
transformed
AS (
        SELECT  --+ USE_HASH(a,b)
            a.SALENO
            ,a.JUCHKBN
            ,a.TORIKEIKBN
            ,a.JUCHDATE
            ,a.KOKYANO
            ,a.HANROCODE
            ,a.SYOHANROBUNNAME
            ,a.CHUHANROBUNNAME
            ,a.DAIHANROBUNNAME
            ,a.SOGOKEI
            ,a.TENPOCODE
            ,a.MEISAINUKIKINGAKU
            ,a.WARIMAENUKIKINGAKU
            ,a.GMEISAINUKIKINGAKU
            ,IFNULL(b.KONYUKAISU,0) AS KONYUKAISU
            ,IFNULL(b.ZAISEKIDAYS,0) AS ZAISEKIDAYS
            ,IFNULL(b.ZAISEKIMONTH,0) AS ZAISEKIMONTH
            ,IFNULL(b.KEIKADAYS,0) AS KEIKADAYS
            ,a.JUCHITEMKBNSHOHIN
            ,a.JUCHITEMKBNTRIAL
            ,a.JUCHITEMKBNSAMPLE
            ,NULL INSERTED_BY
            ,NULL UPDATED_BY
        FROM
        (
            SELECT
                SALENO
                ,JUCHKBN
                ,TORIKEIKBN
                ,JUCHDATE
                ,KOKYANO
                ,HANROCODE
                ,SYOHANROBUNNAME
                ,CHUHANROBUNNAME
                ,DAIHANROBUNNAME
                ,SOGOKEI
                ,TENPOCODE
                ,MEISAINUKIKINGAKU
                ,WARIMAENUKIKINGAKU
                ,GMEISAINUKIKINGAKU
                ,JUCHITEMKBNSHOHIN
                ,JUCHITEMKBNTRIAL
                ,JUCHITEMKBNSAMPLE
            FROM v01kokyakonyu 
            ) a
            LEFT JOIN
            (
            SELECT 
                saleno,
                --何回目の購入か  1/28 2の対応
                DENSE_RANK() OVER (PARTITION BY KOKYANO ORDER BY JUCHDATE) AS KONYUKAISU,
                --在籍日数(初回購入日からの経過日数)
                DATEDIFF(day, 
                        TO_DATE(FIRST_VALUE(JUCHDATE) OVER (PARTITION BY KOKYANO ORDER BY JUCHDATE, SALENO ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)::varchar, 'YYYYMMDD'),
                        TO_DATE(JUCHDATE::varchar, 'YYYYMMDD')
                ) AS ZAISEKIDAYS,
                --在籍月数(初回購入日からの経過月数("数え"で見る)   1/28 1の修正
                FLOOR(MONTHS_BETWEEN(
                        TO_DATE(JUCHDATE::varchar, 'YYYYMMDD'),
                        TO_DATE(FIRST_VALUE(JUCHDATE) OVER (PARTITION BY KOKYANO ORDER BY JUCHDATE, SALENO ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)::varchar, 'YYYYMMDD')
                ) + 1) AS ZAISEKIMONTH,
                --前回購入日からの経過日数
                NVL(
                    DATEDIFF(day, 
                            TO_DATE(MAX(JUCHDATE) OVER (PARTITION BY KOKYANO ORDER BY JUCHDATE ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING)::varchar, 'YYYYMMDD'),
                            TO_DATE(JUCHDATE::varchar, 'YYYYMMDD')
                    ), 
                0) AS KEIKADAYS
            FROM v01kokyakonyu
            WHERE MEISAINUKIKINGAKU > 0
            ) b
        ON(a.SALENO = b.SALENO)
),
final
AS (
  SELECT 
    saleno::varchar(18) as saleno,
    juchkbn::varchar(3) as juchkbn,
    torikeikbn::varchar(3) as torikeikbn,
    juchdate::number(18,0) as juchdate,
    kokyano::varchar(40) as kokyano,
    hanrocode::varchar(60) as hanrocode,
    syohanrobunname::varchar(60) as syohanrobunname,
    chuhanrobunname::varchar(60) as chuhanrobunname,
    daihanrobunname::varchar(60) as daihanrobunname,
    sogokei::number(18,0) as sogokei,
    tenpocode::varchar(7) as tenpocode,
    meisainukikingaku::number(38,0) as meisainukikingaku,
    warimaenukikingaku::number(38,0) as warimaenukikingaku,
    gmeisainukikingaku::number(38,0) as gmeisainukikingaku,
    konyukaisu::number(18,0) as konyukaisu,
    zaisekidays::number(18,0) as zaisekidays,
    zaisekimonth::number(38,0) as zaisekimonth,
    keikadays::number(18,0) as keikadays,
    juchitemkbnshohin::number(38,0) as juchitemkbnshohin,
    juchitemkbntrial::number(38,0) as juchitemkbntrial,
    juchitemkbnsample::number(38,0) as juchitemkbnsample,
    current_timestamp()::timestamp_ntz(9) AS inserted_date,
    inserted_by::varchar(100) as inserted_by,
    current_timestamp()::timestamp_ntz(9) AS updated_date,
    updated_by::varchar(100) as updated_by
  FROM transformed
)
SELECT *
FROM final