with tt02salem_uri_mv as (
select * from dev_dna_core.jpdcledw_integration.tt02salem_uri_mv
),
tt02salem_hen_mv as (
select * from dev_dna_core.jpdcledw_integration.tt02salem_hen_mv
),
transformed as (
select
       saleno as saleno,
       gyono as gyono,
       itemcode as itemcode,
       wariritu as wariritu,
       tanka as tanka,
       warimaekomitanka as warimaekomitanka,
       suryo as suryo,
       kingaku as kingaku,
       warimaekomikingaku as warimaekomikingaku,
       meisainukikingaku as meisainukikingaku,
       warimaenukikingaku as warimaenukikingaku,
       meisaitax as meisaitax,
       dispsaleno as dispsaleno,
       kesaiid as kesaiid,
       trim(saleno) as saleno_trm,
       1 as maker,
       null as salemrowid
 from
       tt02salem_uri_mv tt02salem_uri_mv
 union all
 select
       saleno as saleno,
       gyono as gyono,
       itemcode as itemcode,
       wariritu as wariritu,
       tanka as tanka,
       warimaekomitanka as warimaekomitanka,
       suryo as suryo,
       kingaku as kingaku,
       warimaekomikingaku as warimaekomikingaku,
       meisainukikingaku as meisainukikingaku,
       warimaenukikingaku as warimaenukikingaku,
       meisaitax as meisaitax,
       dispsaleno as dispsaleno,
       kesaiid as kesaiid,
       trim(saleno) as saleno_trm,
       2 as maker,
       null as salemrowid
 from
       tt02salem_hen_mv tt02salem_hen_mv
 ),
final as (
select
saleno::varchar(62) as saleno,
gyono::number(18,0) as gyono,
itemcode::varchar(45) as itemcode,
wariritu::number(18,0) as wariritu,
tanka::number(18,0) as tanka,
warimaekomitanka::number(18,0) as warimaekomitanka,
suryo::number(18,0) as suryo,
kingaku::number(18,0) as kingaku,
warimaekomikingaku::number(18,0) as warimaekomikingaku,
meisainukikingaku::number(18,0) as meisainukikingaku,
warimaenukikingaku::number(18,0) as warimaenukikingaku,
meisaitax::number(18,0) as meisaitax,
dispsaleno::varchar(60) as dispsaleno,
kesaiid::number(10,0) as kesaiid,
saleno_trm::varchar(62) as saleno_trm,
maker::number(18,0) as maker,
salemrowid::varchar(1) as salemrowid,
current_timestamp()::timestamp_ntz(9) as inserted_date,
'etl_batch'::varchar(100) as inserted_by ,
current_timestamp::timestamp_ntz(9) as updated_date,
null::varchar(100) as updated_by
from transformed
)
select * from final
 