with kesai_h_data_mart_mv_kizuna as (
select * from {{ ref('jpndcledw_integration__kesai_h_data_mart_mv') }}
),
transformed as (
select
    saleno                                                       as    saleno
    ,kokyano                                                      as   kokyano
    ,row_number() over(partition by kokyano order by juchdate,saleno)    as   rowno
    ,to_date(juchdate::varchar,'YYYYMMDD')                           as   juchdate
    ,nvl(insertdate,0)                                            as   insertdate
    ,nvl(shukadate_p,0)                                           as   shukadate_p
from kesai_h_data_mart_mv_kizuna
where kokyano is not null                   --juchdate=0のレコードのエラーを防ぐため
and   kokyano not in('0000000011','DUMMY')  --juchdate=0のレコードのエラーを防ぐため
and   sogokei >= 0
--bgn-add 20220415 d.yamashita ***変更30226(frequencyデータの抽出条件修正)**** 
and   maker <> '3'    --3:調整行dummy は除外
--end-add 20220415 d.yamashita ***変更30226(frequencyデータの抽出条件修正)****
and   juchkbn not in ('90','91','92')    --90,91,92:返品データ は除外
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