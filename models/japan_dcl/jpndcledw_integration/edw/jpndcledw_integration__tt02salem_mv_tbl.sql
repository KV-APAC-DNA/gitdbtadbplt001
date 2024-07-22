with TT02SALEM_URI_MV as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TT02SALEM_URI_MV
),
TT02SALEM_HEN_MV as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TT02SALEM_HEN_MV
),
transformed as (
SELECT
       SALENO as SALENO,
       GYONO as GYONO,
       ITEMCODE as ITEMCODE,
       WARIRITU as WARIRITU,
       TANKA as TANKA,
       WARIMAEKOMITANKA as WARIMAEKOMITANKA,
       SURYO as SURYO,
       KINGAKU as KINGAKU,
       WARIMAEKOMIKINGAKU as WARIMAEKOMIKINGAKU,
       MEISAINUKIKINGAKU as MEISAINUKIKINGAKU,
       WARIMAENUKIKINGAKU as WARIMAENUKIKINGAKU,
       MEISAITAX as MEISAITAX,
       DISPSALENO as DISPSALENO,
       KESAIID as KESAIID,
       TRIM(SALENO) as SALENO_TRM,
       1 as maker,
       null as SALEMROWID
 FROM
       TT02SALEM_URI_MV TT02SALEM_URI_MV
 UNION ALL
 SELECT
       SALENO as SALENO,
       GYONO as GYONO,
       ITEMCODE as ITEMCODE,
       WARIRITU as WARIRITU,
       TANKA as TANKA,
       WARIMAEKOMITANKA as WARIMAEKOMITANKA,
       SURYO as SURYO,
       KINGAKU as KINGAKU,
       WARIMAEKOMIKINGAKU as WARIMAEKOMIKINGAKU,
       MEISAINUKIKINGAKU as MEISAINUKIKINGAKU,
       WARIMAENUKIKINGAKU as WARIMAENUKIKINGAKU,
       MEISAITAX as MEISAITAX,
       DISPSALENO as DISPSALENO,
       KESAIID as KESAIID,
       TRIM(SALENO) as SALENO_TRM,
       2 as maker,
       null as SALEMROWID
 FROM
       TT02SALEM_HEN_MV TT02SALEM_HEN_MV
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
 