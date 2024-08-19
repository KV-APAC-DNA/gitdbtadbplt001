with KR_FREQUENCY_1YN_WORK_kizuna as (
select * from {{ ref('jpndcledw_integration__kr_frequency_1yn_work_kizuna') }}
),

transformed as (
SELECT A.SALENO                AS SALENO       --QlikViewに合わせる
      ,A.KOKYANO               AS KOKYANO
      ,A.ROWNO                 AS NOW_ROWNO    --顧客別受注No_1円
      ,B.ROWNO                 AS PRE_ROWNO
      ,A.JUCHDATE              AS NOW_JUCHDATE
      ,B.JUCHDATE              AS PRE_JUCHDATE
      ,datediff(day,B.JUCHDATE, A.JUCHDATE) AS ELAPSED      --経過日数_1円
--BGN-UPD 20200108 D.YAMASHITA ***変更21980(QlikViewのFREQUENCYデータの欠落不具合修正)****
--    ,GREATEST(A.INSERTDATE,B.INSERTDATE)   INSERTDATE
--    ,GREATEST(A.SHUKADATE_P,B.SHUKADATE_P) SHUKADATE_P
      ,GREATEST(NVL(A.INSERTDATE, 0),NVL(B.INSERTDATE, 0)) INSERTDATE
      ,GREATEST(NVL(A.SHUKADATE_P,0),NVL(B.SHUKADATE_P,0)) SHUKADATE_P
--END-UPD 20200108 D.YAMASHITA ***変更21980(QlikViewのFREQUENCYデータの欠落不具合修正)****
FROM   KR_FREQUENCY_1YN_WORK_kizuna A
       LEFT JOIN KR_FREQUENCY_1YN_WORK_kizuna B
              ON B.KOKYANO = A.KOKYANO
             AND B.ROWNO   = A.ROWNO - 1
),
final as (
select
SALENO::VARCHAR(63) as SALENO,
KOKYANO::VARCHAR(30) as KOKYANO,
NOW_ROWNO::NUMBER(38,18) as NOW_ROWNO,
PRE_ROWNO::NUMBER(38,18) as PRE_ROWNO,
NOW_JUCHDATE::TIMESTAMP_NTZ(9) as NOW_JUCHDATE,
PRE_JUCHDATE::TIMESTAMP_NTZ(9) as PRE_JUCHDATE,
ELAPSED::NUMBER(38,18) as ELAPSED,
INSERTDATE::NUMBER(38,18) as INSERTDATE,
SHUKADATE_P::NUMBER(38,18) as SHUKADATE_P,
current_timestamp()::timestamp_ntz(9) as INSERTED_DATE,
null::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) as UPDATED_DATE,
null::varchar(100) as updated_by
from transformed
)
select * from final