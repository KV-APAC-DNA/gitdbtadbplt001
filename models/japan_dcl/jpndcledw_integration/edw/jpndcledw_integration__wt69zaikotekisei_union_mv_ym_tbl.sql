with tsuhan_gassan_mv as (
select * from dev_dna_core.snapjpdcledw_integration.tsuhan_gassan_mv
),
oroshi_gassan_mv as (
select * from dev_dna_core.snapjpdcledw_integration.oroshi_gassan_mv
),
transformed as (
select
uni.shukadate,
uni.shukaym, 
uni.channel, 
uni.itemcode, 
uni.gokei
from
(
select
ors.shukadate
,substring(ors.shukadate,1,6) shukaym
, ors.channel
, ors.itemcode
, ors.suryo + ors.hensu as gokei
from oroshi_gassan_mv ors
union all
select
uri.shukadate
,substring(uri.shukadate,1,6) shukaym
, uri.channel
, uri.itemcode
, uri.suryo + uri.hensu as gokei
from tsuhan_gassan_mv uri
)uni
),
final as (
select
shukadate::number(18,0) as shukadate,
shukaym::varchar(18) as shukaym,
channel::varchar(3) as channel,
itemcode::varchar(45) as itemcode,
gokei::number(18,0) as gokei,
current_timestamp()::timestamp_ntz(9) as inserted_date,
'ETL_Batch'::varchar(100)  as inserted_by,
current_timestamp()::timestamp_ntz(9) as updated_date ,
null::varchar(100) as updated_by
from transformed
)
select * from final

