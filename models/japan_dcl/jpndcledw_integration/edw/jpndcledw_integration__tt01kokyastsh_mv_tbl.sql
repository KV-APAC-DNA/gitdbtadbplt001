WITH tt01kokyastsh_ikou
AS (
  SELECT *
  FROM {{ source('jpdcledw_integration','tt01kokyastsh_ikou') }}
  ),
tt01saleh_mv_mt
AS (
  SELECT *
  FROM  {{ ref('jpndcledw_integration__tt01saleh_mv_mt') }}
  ),
transformed
AS (
  SELECT
         TT01.SALENO					AS SALENO
        ,TT01.JUCHKBN					AS JUCHKBN
        ,TT01.JUCHDATE					AS JUCHDATE
        ,TT01.KOKYANO					AS KOKYANO
        ,TT01.TORIKEIKBN				AS TORIKEIKBN
        ,TT01.CANCELFLG					AS CANCELFLG
        ,TT01.HANROCODE					AS HANROCODE
        ,TT01.SYOHANROBUNNAME			AS SYOHANROBUNNAME
        ,TT01.CHUHANROBUNNAME			AS CHUHANROBUNNAME
        ,TT01.DAIHANROBUNNAME			AS DAIHANROBUNNAME
        ,TT01.SOGOKEI					AS SOGOKEI
        ,TT01.TENPOCODE					AS TENPOCODE
        ,TT01.SALENO					AS SALENO_TRM
        ,1								AS maker
        ,null						AS SALEHROWID
        FROM tt01kokyastsh_ikou TT01
        UNION ALL
        SELECT
        TT01.SALENO					AS SALENO
        ,TT01.JUCHKBN					AS JUCHKBN
        ,TT01.JUCHDATE					AS JUCHDATE
        ,TT01.KOKYANO					AS KOKYANO
        ,TT01.TORIKEIKBN				AS TORIKEIKBN
        ,cast(TT01.CANCELFLG as numeric)		AS CANCELFLG
        ,TT01.HANROCODE					AS HANROCODE
        ,TT01.SYOHANROBUNNAME			AS SYOHANROBUNNAME
        ,TT01.CHUHANROBUNNAME			AS CHUHANROBUNNAME
        ,TT01.DAIHANROBUNNAME			AS DAIHANROBUNNAME
        ,TT01.SOGOKEI					AS SOGOKEI
        ,TT01.TENPOCODE					AS TENPOCODE
        ,TT01.SALENO_TRM				AS SALENO_TRM
        ,2								AS maker
        ,null						AS SALEHROWID
        --BGN-MOD 20180507 HIGASHI ***変更管理202 顧客ステータス計算の計算方法の見直し(返品の扱い変更)****
        FROM tt01saleh_mv_mt TT01
        --FROM CI_DWH_MAIN.TT01SALEH_MV TT01
        --END-MOD 20180507 HIGASHI ***変更管理202 顧客ステータス計算の計算方法の見直し(返品の扱い変更)****
  ),
final
AS (
  SELECT 
        saleno::varchar(62) as saleno,
        juchkbn::varchar(3) as juchkbn,
        juchdate::number(18,0) as juchdate,
        kokyano::varchar(30) as kokyano,
        torikeikbn::varchar(3) as torikeikbn,
        cancelflg::number(18,0) as cancelflg,
        hanrocode::varchar(60) as hanrocode,
        syohanrobunname::varchar(60) as syohanrobunname,
        chuhanrobunname::varchar(60) as chuhanrobunname,
        daihanrobunname::varchar(60) as daihanrobunname,
        sogokei::number(18,0) as sogokei,
        tenpocode::varchar(8) as tenpocode,
        saleno_trm::varchar(62) as saleno_trm,
        maker::number(18,0) as maker,
        salehrowid::varchar(1) as salehrowid
FROM transformed
)
SELECT *
FROM final