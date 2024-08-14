with KR_FREQUENCY_ALL_WORK_kizuna as (
select * from {{ ref('jpndcledw_integration__kr_frequency_all_work_kizuna') }}
),
KR_FREQUENCY_ALL_WORK_kizuna as (
select * from {{ ref('jpndcledw_integration__kr_frequency_all_work_kizuna') }}
),
transformed as (
SELECT A.SALENO                AS SALENO       --QlikViewに合わせる
      ,A.KOKYANO               AS KOKYANO
      ,A.ROWNO                 AS NOW_ROWNO    --顧客別受注No_全件
      ,B.ROWNO                 AS PRE_ROWNO
      ,A.JUCHDATE              AS NOW_JUCHDATE
      ,B.JUCHDATE              AS PRE_JUCHDATE
      ,datediff(day,B.JUCHDATE, A.JUCHDATE) AS ELAPSED      --経過日数_全件
--BGN-UPD 20200108 D.YAMASHITA ***変更21980(QlikViewのFREQUENCYデータの欠落不具合修正)****
--    ,GREATEST(A.INSERTDATE,B.INSERTDATE)   INSERTDATE
--    ,GREATEST(A.SHUKADATE_P,B.SHUKADATE_P) SHUKADATE_P
      ,GREATEST(NVL(A.INSERTDATE, 0),NVL(B.INSERTDATE, 0)) INSERTDATE
      ,GREATEST(NVL(A.SHUKADATE_P,0),NVL(B.SHUKADATE_P,0)) SHUKADATE_P
--END-UPD 20200108 D.YAMASHITA ***変更21980(QlikViewのFREQUENCYデータの欠落不具合修正)****
FROM   KR_FREQUENCY_ALL_WORK_kizuna A
       LEFT JOIN KR_FREQUENCY_ALL_WORK_kizuna B
              ON B.KOKYANO = A.KOKYANO
             AND B.ROWNO   = A.ROWNO - 1
),
final as (
select 
saleno::varchar(63) as saleno,
kokyano::varchar(30) as kokyano,
now_rowno::number(38,18) as now_rowno,
pre_rowno::number(38,18) as pre_rowno,
now_juchdate::timestamp_ntz(9) as now_juchdate,
pre_juchdate::timestamp_ntz(9) as pre_juchdate,
elapsed::number(38,18) as elapsed,
insertdate::number(38,18) as insertdate,
shukadate_p::number(38,18) as shukadate_p,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final
