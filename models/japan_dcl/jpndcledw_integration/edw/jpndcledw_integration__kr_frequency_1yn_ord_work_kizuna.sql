with KESAI_H_DATA_MART_MV_kizuna as (
select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv_kizuna') }}
),
transformed as (
SELECT SALENO                                                      AS    SALENO
      ,KOKYANO                                                     AS    KOKYANO
      ,ROW_NUMBER() OVER(PARTITION BY KOKYANO ORDER BY JUCHDATE,SALENO)   AS    ROWNO
      ,to_date(JUCHDATE::varchar,'YYYYMMDD')                         AS    JUCHDATE
      ,NVL(INSERTDATE,0)                                           AS    INSERTDATE
      ,NVL(SHUKADATE_P,0)                                          AS    SHUKADATE_P
FROM KESAI_H_DATA_MART_MV_kizuna
WHERE KOKYANO IS NOT NULL                   --JUCHDATE=0のレコードのエラーを防ぐため
AND   KOKYANO NOT IN('0000000011','DUMMY')  --JUCHDATE=0のレコードのエラーを防ぐため
AND   SOGOKEI > 0
AND   JUCHKBN IN ('0','2')
--BGN-ADD 20220415 D.YAMASHITA ***変更30226(FREQUENCYデータの抽出条件修正)****
AND   MAKER <> '3'    --3:調整行DUMMY は除外
--END-ADD 20220415 D.YAMASHITA ***変更30226(FREQUENCYデータの抽出条件修正)****
),
final as (
select
saleno::varchar(63) as saleno,
kokyano::varchar(30) as kokyano,
rowno::number(38,18) as rowno,
juchdate::timestamp_ntz(9) as juchdate,
insertdate::number(38,18) as insertdate,
shukadate_p::number(38,18) as shukadate_p,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final